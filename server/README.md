# disco server (discod)

`discod` is intended for local development and testing.

`discod` is a thin wrapper around a real, embedded etcd server
(`go.etcd.io/etcd/server/v3/embed`). It exists so you can run any of
disco's coordination primitives (via `provider/etcd`) against a single,
self-contained binary instead of standing up a full etcd cluster —
without disco maintaining its own storage engine or wire protocol.
Because it's genuinely etcd on the wire, any unmodified `clientv3.Client`
(and therefore `provider/etcd`, unmodified) talks to it exactly as it
would talk to a real etcd cluster.

## Quick start

```bash
# From the repo root:
go run ./server/src/cmd/discod
```

By default this listens on etcd's own conventional ports —
`127.0.0.1:2379` for clients, `127.0.0.1:2380` for peers — storing its
data under `./discod-data`.

| Flag                     | Default                 | Meaning                               |
|--------------------------|-------------------------|---------------------------------------|
| `-name`                  | `default`               | etcd member name                      |
| `-data-dir`              | `discod-data`           | Directory for etcd's data             |
| `-listen-client-urls`    | `http://127.0.0.1:2379` | Client URL to listen on and advertise |
| `-listen-peer-urls`      | `http://127.0.0.1:2380` | Peer URL to listen on and advertise   |
| `-cert-file`             | `""`                    | TLS cert file for the client listener |
| `-key-file`              | `""`                    | TLS key file for the client listener  |
| `-trusted-ca-file`       | `""`                    | Trusted CA file for verifying clients |
| `-client-cert-auth`      | `false`                 | Require a valid client certificate    |
| `-peer-cert-file`        | `""`                    | TLS cert file for the peer listener   |
| `-peer-key-file`         | `""`                    | TLS key file for the peer listener    |
| `-peer-trusted-ca-file`  | `""`                    | Trusted CA file for verifying peers   |
| `-peer-client-cert-auth` | `false`                 | Require a valid peer certificate      |

Set `-listen-client-urls`/`-listen-peer-urls` to `https://` when passing
cert/key flags — the flags configure the TLS material but don't rewrite
the URL scheme for you.

Shut it down with `Ctrl-C` (SIGINT) or `SIGTERM` for a graceful stop.

## Using it as a disco backend

Point `provider/etcd` at it exactly like a real etcd cluster — nothing
about `lock.Service`, `rwlock.Service`, `semaphore.Service`, or
`election.Service` changes:

```bash
go run ./server/src/cmd/discod    # in one terminal, listening on 127.0.0.1:2379

go run ./examples/lock/db         # in another — unmodified etcd-backed example
go run ./examples/election/basic  # likewise
```

```go
clientv3 "go.etcd.io/etcd/client/v3"
etcdprovider "github.com/ahrtr/disco/provider/etcd"

cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:2379"}})
defer cli.Close()

svc, _ := etcdprovider.NewLock(cli, "/locks/my-resource")
defer svc.Close()
```

See the root [`README.md`](../README.md) for the full lock/rwlock/semaphore/
election quick-start — all of it works against `discod` unchanged.

