# disco

disco is a Go library for distributed coordination. It provides lease-based
distributed locks backed by etcd, with built-in fencing token support to
guarantee that stale clients — those whose lease has already expired — can
never corrupt a shared resource.

The core problem disco solves: a process can acquire a lock, get paused (long
GC pause, network partition, VM suspend, etc.), and wake up still believing
it is the current owner. Without fencing, it would freely write to the shared
resource. With fencing, every lock acquisition issues a monotonically increasing
token; the resource rejects any write whose token is lower than the highest it
has already accepted, so the zombie's write is safely rejected regardless of how
long it was paused.

disco is designed to be extensible. The lock backend is abstracted behind the
`lock.Service` interface; `provider/etcd` is the first implementation, with
ZooKeeper and Redis planned. The `provider` package is shared across features,
so future coordination primitives (leader election, barriers, etc.) can reuse
the same backend.

## Three-party contract

Safety is a shared responsibility across three parties:

- The **lock service** is only responsible for assigning who is the latest owner.
- The **client** (lock holder) must attach the fencing token to every request it sends to a guarded resource.
- The **resource** must reject stale owners by comparing the fencing token.

Each party has exactly one responsibility. The guarantee only holds when all three honour their part.

## Workflow

```
  Client                    Lock Service             Resource
  (lock holder)             (etcd)                   (DB / External API)
       |                          |                          |
       |-- (1) acquire lock ----> |                          |
       |<-- fencing token --------|                          |
       |                          |                          |
       |-- (2) request + token --------------------------> |
       |                          |    (3) validate token    |
       |                          |        token >= mark:    |
       |<-- accepted ----------------------------------------|
       |        OR                |        token <  mark:    |
       |<-- rejected (stale) --------------------------------|
```

**(1)** The client calls `svc.Lock()`. The lock service grants an exclusive
lease and returns a monotonically increasing **fencing token**.

**(2)** The client attaches the fencing token to every request it sends to
the protected resource (via HTTP header or gRPC metadata).

**(3)** The resource validates the token against its **high-water mark**:
if `token >= mark` the request is accepted and the mark advances; if
`token < mark` the request is rejected — the caller is a stale owner.

## Directory layout

```
disco/
├── lock/                   # Distributed lock — Service interface, Grant, errors
│   ├── fencing/            # Token type + HTTP/gRPC transport helpers
│   └── guard/              # Server-side validator: high-water mark, HTTP middleware, gRPC interceptors
├── provider/               # Backend implementations (shared across features)
│   └── etcd/               # etcd backend; zookeeper/redis planned
└── examples/
    ├── db/                 # Direct DB protection: fencing token stored and checked inside the DB
    ├── http/
    │   ├── resource/       # HTTP resource server protected by guard middleware
    │   └── client/         # HTTP client: zombie scenario over HTTP
    └── grpc/
        ├── pb/             # gRPC service definition (JSON codec, no protoc required)
        ├── resource/       # gRPC resource server protected by guard interceptor
        └── client/         # gRPC client: zombie scenario over gRPC
```

## How fencing tokens work

Every lock acquisition returns a **strictly increasing** integer — the etcd
cluster revision at the moment the lock is acquired. Resources track the
highest token they have ever seen. Requests from older owners (lower token)
are rejected:

```
Client A gets token 34  ──► writes to DB (token 34 accepted)
Client A's lease expires ──► Client B gets token 51
Client A reappears as zombie ──► writes to DB with token 34 → REJECTED (34 < 51)
```

## Quick start

### Lock holder (client side)

```go
import (
    "log"
    "net/http"

    clientv3 "go.etcd.io/etcd/client/v3"
    "google.golang.org/grpc/metadata"
    "github.com/ahrtr/disco/lock/fencing"
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

### Resource guard (server side)

```go
import "github.com/ahrtr/disco/lock/guard"

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

| Decision                          | Rationale                                                                                                                                                                       |
|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Cluster revision as fencing token | etcd cluster revision is globally ordered and increases on every write; the revision recorded when the lock is acquired is always strictly higher than any previous acquisition |
| Provider manages keepalive        | The session's keepalive goroutine runs internally; callers watch `svc.Done()` instead of calling `Renew()`                                                                      |
| `Guard` high-water mark           | Atomic CAS loop with no locks; accepts `token >= mark`, rejects `token < mark`                                                                                                  |
| Caller-owned etcd client          | The caller creates, configures, and closes the etcd client; the provider never closes it                                                                                        |
| No `init()` auto-registration     | Providers are constructed explicitly; no hidden init-time side effects                                                                                                          |

## Running examples

```bash
# Start etcd (Docker):
docker run -d -p 2379:2379 gcr.io/etcd-development/etcd:v3.6.0 \
  etcd --advertise-client-urls http://0.0.0.0:2379 \
       --listen-client-urls http://0.0.0.0:2379

# Direct DB protection (fencing token stored inside the database):
go run ./examples/db

# HTTP zombie scenario (two terminals):
go run ./examples/http/resource
go run ./examples/http/client

# gRPC zombie scenario (two terminals):
go run ./examples/grpc/resource
go run ./examples/grpc/client
```
