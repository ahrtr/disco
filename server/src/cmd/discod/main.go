// Command discod is a thin wrapper around a real, embedded etcd server
// (go.etcd.io/etcd/server/v3/embed). It exists so disco's examples and
// users can run a single, self-contained binary instead of standing up a
// full etcd cluster — provider/etcd (and any unmodified clientv3.Client)
// talks to it exactly as it would talk to a real etcd cluster, because it
// is one. See server/README.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/server/v3/embed"
)

func main() {
	name := flag.String("name", "default", "etcd member name")
	dataDir := flag.String("data-dir", "discod-data", "directory for etcd data")
	clientURL := flag.String("listen-client-urls", "http://127.0.0.1:2379", "client URL to listen on and advertise")
	peerURL := flag.String("listen-peer-urls", "http://127.0.0.1:2380", "peer URL to listen on and advertise")

	certFile := flag.String("cert-file", "", "TLS cert file for the client listener")
	keyFile := flag.String("key-file", "", "TLS key file for the client listener")
	trustedCAFile := flag.String("trusted-ca-file", "", "trusted CA file for verifying clients")
	clientCertAuth := flag.Bool("client-cert-auth", false, "require a valid client certificate")

	peerCertFile := flag.String("peer-cert-file", "", "TLS cert file for the peer listener")
	peerKeyFile := flag.String("peer-key-file", "", "TLS key file for the peer listener")
	peerTrustedCAFile := flag.String("peer-trusted-ca-file", "", "trusted CA file for verifying peers")
	peerClientCertAuth := flag.Bool("peer-client-cert-auth", false, "require a valid peer certificate")

	flag.Parse()

	clientTLSInfo := transport.TLSInfo{
		CertFile:       *certFile,
		KeyFile:        *keyFile,
		TrustedCAFile:  *trustedCAFile,
		ClientCertAuth: *clientCertAuth,
	}
	peerTLSInfo := transport.TLSInfo{
		CertFile:       *peerCertFile,
		KeyFile:        *peerKeyFile,
		TrustedCAFile:  *peerTrustedCAFile,
		ClientCertAuth: *peerClientCertAuth,
	}

	if err := run(*name, *dataDir, *clientURL, *peerURL, clientTLSInfo, peerTLSInfo); err != nil {
		log.Fatal(err)
	}
}

func run(name, dataDir, clientURL, peerURL string, clientTLSInfo, peerTLSInfo transport.TLSInfo) error {
	cURL, err := url.Parse(clientURL)
	if err != nil {
		return fmt.Errorf("parse client URL: %w", err)
	}
	pURL, err := url.Parse(peerURL)
	if err != nil {
		return fmt.Errorf("parse peer URL: %w", err)
	}

	cfg := embed.NewConfig()
	cfg.Name = name
	cfg.Dir = dataDir
	cfg.ListenClientUrls = []url.URL{*cURL}
	cfg.AdvertiseClientUrls = []url.URL{*cURL}
	cfg.ListenPeerUrls = []url.URL{*pURL}
	cfg.AdvertisePeerUrls = []url.URL{*pURL}
	cfg.InitialCluster = fmt.Sprintf("%s=%s", name, peerURL)
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.ClientTLSInfo = clientTLSInfo
	cfg.PeerTLSInfo = peerTLSInfo

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return fmt.Errorf("start embedded etcd: %w", err)
	}
	defer e.Close()

	select {
	case <-e.Server.ReadyNotify():
		log.Printf("discod (embedded etcd) listening on %s (data dir %s)", clientURL, dataDir)
	case <-time.After(60 * time.Second):
		e.Server.Stop()
		return fmt.Errorf("embedded etcd took too long to start")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	select {
	case <-ctx.Done():
		log.Print("shutting down...")
		return nil
	case err := <-e.Err():
		return err
	}
}
