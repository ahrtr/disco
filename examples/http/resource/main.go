// Command resource is a simple HTTP resource server protected by a fencing
// token guard. It is the resource-side counterpart to the http/client example.
//
// Every write request must carry a valid X-Fencing-Token header. A token lower
// than the highest token already accepted is rejected with 409 Conflict,
// preventing stale lock owners from corrupting the resource.
//
// Run the resource server:
//
//	go run ./examples/http/resource
//
// Then run the client in a separate terminal:
//
//	go run ./examples/http/client
//
// Or test manually with curl (first request sets high-water to 42, second with
// 41 is rejected):
//
//	curl -i -X POST -H "X-Fencing-Token: 42" http://localhost:8080/write
//	curl -i -X POST -H "X-Fencing-Token: 41" http://localhost:8080/write
//	curl -i -X POST -H "X-Fencing-Token: 43" http://localhost:8080/write
package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/ahrtr/disco/lock/guard"
)

func main() {
	g := guard.New()

	mux := http.NewServeMux()

	// /write is protected by the guard middleware: the token is validated before
	// the handler runs. /status is deliberately left unguarded so you can
	// observe the high-water mark without a token.
	mux.Handle("/write", g.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fmt.Fprintf(w, "write accepted; high-water=%d\n", g.HighWater())
	})))
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "high-water mark: %d\n", g.HighWater())
	})

	log.Println("http/resource listening on :8080")
	log.Println("  POST /write  – requires X-Fencing-Token header")
	log.Println("  GET  /status – shows current high-water mark (no token needed)")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
