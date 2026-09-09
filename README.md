# disco

disco is a Go library for distributed coordination. It provides lease-based
distributed locks, read/write locks, counting semaphores, and leader
election backed by etcd, with built-in fencing token support to guarantee
that stale owners — clients or leaders whose lease has already expired —
can never corrupt a shared resource.

The core problem disco solves: a process can acquire a lock or win a leader
election, get paused (long GC pause, network partition, VM suspend, etc.), and
wake up still believing it is the current owner. Without fencing, it would
freely write to the shared resource. With fencing, every lock acquisition or
leadership term issues a monotonically increasing token; the resource rejects
any write whose token is lower than the highest it has already accepted, so
the zombie's write is safely rejected regardless of how long it was paused.

disco is designed to be extensible. Each primitive is abstracted behind its
own interface — `lock.Service` for mutual exclusion, `rwlock.Service` for
read/write locking, `semaphore.Service` for counting semaphores,
`election.Service` for leader election — and all four share `provider/etcd`
as their first backend implementation, with ZooKeeper and Redis planned.
The `provider` package is shared across features, so future coordination
primitives (barriers, etc.) can reuse the same backend.

## Supported primitives

- **Lock** ([`lock.Service`](lock/service.go)) — exclusive mutual exclusion: only one holder at a time.
- **RWLock** ([`rwlock.Service`](rwlock/service.go)) — read/write lock: any number of concurrent readers, or one exclusive writer.
- **Semaphore** ([`semaphore.Service`](semaphore/service.go)) — counting semaphore: up to a fixed number of concurrent permit holders.
- **Election** ([`election.Service`](election/service.go)) — leader election: candidates campaign, one becomes leader, others can passively observe who currently holds leadership.

## Three-party contract

Safety is a shared responsibility across three parties. This contract is
identical across all primitives — only the vocabulary changes (lock holder
vs. leader vs. reader/writer vs. permit holder):

- The **coordination service** (`lock.Service`, `rwlock.Service`, `semaphore.Service`, or `election.Service`) is only responsible for assigning who is the latest owner.
- The **client** (lock holder, rwlock holder, permit holder, or elected leader) must attach the fencing token to every request it sends to a guarded resource.
- The **resource** must reject stale owners by comparing the fencing token.

Each party has exactly one responsibility. The guarantee only holds when all three honour their part.

## Workflow

```
         Client                               Coordination Service                    Resource
 (lock holder / leader)                              (etcd)                     (DB / external API)
            |                                           |                                 |
            |  (1) acquire lock, or win election        |                                 |
            |------------------------------------------>|                                 |
            |<----------------------------------------- |                                 |
            |  fencing token                            |                                 |
            |                                           |                                 |
            |  (2) request + fencing token                
            |---------------------------------------------------------------------------->|
            |                                           |  (3) validate token
            |                                           |  against high-water mark
            |                                                                             |
            |<----------------------------------------------------------------------------|
               accepted   (token >= mark)
            |                                                                             |
            |<----------------------------------------------------------------------------|
               rejected — stale  (token < mark)
```

**(1)** The client calls `svc.Lock()` (mutual exclusion) or `svc.Campaign()`
(leader election). The coordination service grants exclusive ownership and
returns a monotonically increasing **fencing token** — `grant.Token()` for a
lock, `leadership.Token()` for an election.

**(2)** The client attaches the fencing token to every request it sends to
the protected resource (via HTTP header or gRPC metadata).

**(3)** The resource validates the token against its **high-water mark**:
if `token >= mark` the request is accepted and the mark advances; if
`token < mark` the request is rejected — the caller is a stale owner.

## Directory layout

```
disco/
├── lock/                   # Distributed lock — Service interface, Grant, errors
├── rwlock/                 # Distributed read/write lock — Service interface, Grant, errors
├── semaphore/              # Distributed counting semaphore — Service interface, Grant, Stat, errors
├── election/               # Leader election — Service interface, Leadership, errors
├── fencing/                # Token type + HTTP/gRPC transport helpers (shared by lock, rwlock, semaphore, and election)
│   └── guard/              # Server-side validator: high-water mark, HTTP middleware, gRPC interceptors
├── provider/               # Backend implementations (shared across features)
│   └── etcd/               # etcd backend; zookeeper/redis planned
└── examples/
    ├── lock/
    │   ├── db/             # Direct DB protection: fencing token stored and checked inside the DB
    │   ├── http/
    │   │   ├── resource/   # HTTP resource server protected by guard middleware
    │   │   └── client/     # HTTP client: zombie scenario over HTTP
    │   └── grpc/
    │       ├── pb/         # gRPC service definition (JSON codec, no protoc required)
    │       ├── resource/   # gRPC resource server protected by guard interceptor
    │       └── client/     # gRPC client: zombie scenario over gRPC
    ├── rwlock/
    │   └── basic/          # Concurrent readers, an exclusive writer, and a writer zombie/fencing handoff
    ├── semaphore/
    │   └── basic/          # Fixed permit pool, a blocked worker, and a permit-holder zombie/fencing handoff
    └── election/
        └── basic/          # Leader election, failover, and fencing token handoff between leaders
```

> `provider/etcd`'s mutex and election logic started from etcd's own
> `client/v3/concurrency` package, then went through a substantial refactor:
> extracted behind the `lock.Service`/`election.Service` interfaces, wired
> into disco's session/keepalive and fencing-token model, and adjusted in
> places (e.g. cleanup on a failed campaign) to close gaps in the original.
> `rwlock`'s algorithm follows etcd's own
> `client/v3/experimental/recipes.RWMutex` the same way — readers and
> writers each register a key in their own sub-namespace under the lock
> prefix, and reuse the exact same wait-for-predecessors primitive
> `lock.Service`'s mutex already needed, just scoped to a different prefix
> depending on the caller's role. `semaphore` in turn generalizes the mutex
> protocol from a single owner to a fixed number of permits: instead of
> deciding "am I the first-created key under the prefix", it decides "are
> there at most `limit` keys registered", and instead of waiting for every
> predecessor to release, it waits only until enough of them have.

## How fencing tokens work

Every lock acquisition, permit acquisition, or election term returns a
**strictly increasing** integer, though the primitives derive it differently:

- `lock.Grant.FencingToken` and `semaphore.Grant.FencingToken` are the etcd cluster revision at the moment the lock or permit is acquired.
- `election.Leadership.FencingToken` (returned by `Leadership.Token()`) is the `CreateRevision` of the key that won the current leadership term. Candidates may create their keys concurrently and queue up; a key is only recognized as leader once every other still-existing key with a lower `CreateRevision` has been removed, so each successive leadership term's token is always strictly higher than the one it replaced.

Both are globally ordered within the etcd cluster, so either works as a
fencing token: resources track the highest token they have ever seen, and
requests from older owners (lower token) are rejected:

```
Owner A gets token 34  ──► writes to DB (token 34 accepted)
Owner A's lease expires ──► Owner B gets token 51
Owner A reappears as zombie ──► writes to DB with token 34 → REJECTED (34 < 51)
```

## Quick start

### Lock holder (client side)

```go
import (
    "log"
    "net/http"

    clientv3 "go.etcd.io/etcd/client/v3"
    "google.golang.org/grpc/metadata"
    "github.com/ahrtr/disco/fencing"
    etcdprovider "github.com/ahrtr/disco/provider/etcd"
)

// The caller creates and owns the etcd client.
cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
defer cli.Close()

svc, _ := etcdprovider.NewLock(cli, "/locks/my-resource")
defer svc.Close()

// React to involuntary lease loss in the background.
// Done is a property of the service lifetime, not of any individual Lock call.
go func() {
    <-svc.Done()
    log.Printf("lock lost: %v", svc.Err())
    // stop accessing the resource
}()

// Blocking acquire — returns a Grant with the fencing token and lease metadata.
grant, err := svc.Lock(ctx)
if err != nil { ... }
defer svc.Unlock(ctx)

// Stamp every resource request with the fencing token.
req, _ := http.NewRequest("POST", resourceURL, body)
fencing.InjectHTTP(req, grant.Token())

// For gRPC:
outCtx := metadata.NewOutgoingContext(ctx, fencing.ToGRPCMetadata(grant.Token()))
```

### Read/write lock (client side)

```go
import (
    "errors"
    "log"

    clientv3 "go.etcd.io/etcd/client/v3"
    etcdprovider "github.com/ahrtr/disco/provider/etcd"
    "github.com/ahrtr/disco/rwlock"
)

cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
defer cli.Close()

svc, _ := etcdprovider.NewRWLock(cli, "/rwlocks/my-resource")
defer svc.Close()

go func() {
    <-svc.Done()
    log.Printf("rwlock lost: %v", svc.Err())
}()

// Concurrent readers never block each other...
grant, err := svc.RLock(ctx)
if err != nil { ... }
defer svc.RUnlock(ctx)

// ...but Lock blocks until every reader and writer registered before it
// has released, and RLock blocks until every writer registered before it
// has released. Both return a Grant with a fencing token, exactly like
// lock.Service — concurrent readers all wait for the same last writer to
// release before proceeding, so they hold identical tokens.
wgrant, err := svc.Lock(ctx)
if err != nil { ... }
defer svc.Unlock(ctx)

// TryLock/TryRLock attempt the same thing without blocking, returning
// rwlock.ErrRWLockTaken immediately if something is in the way.
if _, err := svc.TryLock(ctx); errors.Is(err, rwlock.ErrRWLockTaken) {
    // held by someone else right now — try again later
}
```

### Semaphore (client side)

```go
import (
    "errors"
    "log"

    clientv3 "go.etcd.io/etcd/client/v3"
    etcdprovider "github.com/ahrtr/disco/provider/etcd"
    "github.com/ahrtr/disco/semaphore"
)

cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
defer cli.Close()

// Up to 3 concurrently held permits on this key.
svc, _ := etcdprovider.NewSemaphore(cli, "/semaphores/my-pool", 3)
defer svc.Close()

go func() {
    <-svc.Done()
    log.Printf("semaphore lost: %v", svc.Err())
}()

// Blocking acquire — returns a Grant with the fencing token, exactly like
// lock.Service's Lock, once fewer than the pool's limit are held.
grant, err := svc.Acquire(ctx)
if err != nil { ... }
defer svc.Release(ctx)

// TryAcquire attempts the same thing without blocking, returning
// semaphore.ErrNoPermitsAvailable immediately if the pool is full.
if _, err := svc.TryAcquire(ctx); errors.Is(err, semaphore.ErrNoPermitsAvailable) {
    // every permit is held right now — try again later
}

// Stat reports current usage, e.g. for a capacity dashboard.
stat, _ := svc.Stat(ctx)
log.Printf("%d/%d permits held", stat.Held(), stat.Limit)
```

### Leader election (client side)

```go
import (
    "log"
    "net/http"

    clientv3 "go.etcd.io/etcd/client/v3"
    "github.com/ahrtr/disco/fencing"
    etcdprovider "github.com/ahrtr/disco/provider/etcd"
)

cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
defer cli.Close()

svc, _ := etcdprovider.NewElection(cli, "/elections/my-service")
defer svc.Close()

// Same Done/Err contract as lock.Service: react to involuntary lease loss.
go func() {
    <-svc.Done()
    log.Printf("leadership lost: %v", svc.Err())
    // stop acting as leader
}()

// Campaign blocks until this candidate wins the election.
if err := svc.Campaign(ctx, "node-a"); err != nil { ... }
defer svc.Resign(ctx)

// Stamp every resource request with the fencing token, exactly like a lock Grant.
leader, _ := svc.Leader(ctx)
req, _ := http.NewRequest("POST", resourceURL, body)
fencing.InjectHTTP(req, leader.Token())

// Any process — not just candidates — can passively watch who the current
// leader is, without ever campaigning itself:
for l := range svc.Observe(ctx) {
    log.Printf("leader is now %q", l.Value)
}
```

### Resource guard (server side)

`Guard` only cares about the `fencing.Token` value, so the same guard protects
resources for both lock holders and elected leaders — no separate code path
needed.

```go
import "github.com/ahrtr/disco/fencing/guard"

g := guard.New()

// As HTTP middleware:
http.Handle("/write", g.HTTPMiddleware(writeHandler))

// As gRPC interceptors:
grpc.NewServer(
    grpc.UnaryInterceptor(g.UnaryInterceptor()),
    grpc.StreamInterceptor(g.StreamInterceptor()),
)

// Or manually:
if err := g.Check(incomingToken); err != nil {
    // errors.Is(err, fencing.ErrTokenStale) → reject
}
```

## Key design decisions

| Decision                                         | Rationale                                                                                                                                                                                                                                    |
|--------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Cluster revision as lock/semaphore fencing token | etcd cluster revision is globally ordered and increases on every write; the revision recorded when the lock or permit is acquired is always strictly higher than any previous acquisition                                                    |
| `CreateRevision` as election fencing token       | A candidate's key can be created at any time and simply queues up; it's only recognized as leader once every lower-`CreateRevision` key has been removed, so each leadership term's token is always strictly higher than the one it replaced |
| Semaphore is a count check, not a rank check     | `limit` keys registered under the prefix means everyone's holding a permit; waiting only needs enough predecessors gone, not all of them — the direct generalization of mutex's "am I the first-created key" (limit 1) check                 |
| Provider manages keepalive                       | The session's keepalive goroutine runs internally; callers watch `svc.Done()` instead of calling `Renew()`                                                                                                                                   |
| `Guard` high-water mark                          | Atomic CAS loop with no locks; accepts `token >= mark`, rejects `token < mark`; shared by lock, rwlock, semaphore, and election since it only validates `fencing.Token`                                                                      |
| Caller-owned etcd client                         | The caller creates, configures, and closes the etcd client; the provider never closes it                                                                                                                                                     |
| No `init()` auto-registration                    | Providers are constructed explicitly; no hidden init-time side effects                                                                                                                                                                       |

## Running examples

```bash
# Start etcd (Docker):
docker run -d -p 2379:2379 gcr.io/etcd-development/etcd:v3.7.1 \
  etcd --advertise-client-urls http://0.0.0.0:2379 \
       --listen-client-urls http://0.0.0.0:2379

# Direct DB protection (fencing token stored inside the database):
go run ./examples/lock/db

# HTTP zombie scenario (two terminals):
go run ./examples/lock/http/resource
go run ./examples/lock/http/client

# gRPC zombie scenario (two terminals):
go run ./examples/lock/grpc/resource
go run ./examples/lock/grpc/client

# Leader election, failover, and fencing token handoff between leaders:
go run ./examples/election/basic

# Read/write lock: concurrent readers, an exclusive writer, and a writer
# zombie/fencing handoff:
go run ./examples/rwlock/basic

# Semaphore: a fixed permit pool, a blocked worker, and a permit-holder
# zombie/fencing handoff:
go run ./examples/semaphore/basic
```
