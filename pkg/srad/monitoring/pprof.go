package monitoring

import (
	"context"
	"log"
	"net/http"
	"net/http/pprof"
)

// StartPprofServer starts an HTTP server with pprof handlers bound to the provided address.
// Example address values: ":6060" or "127.0.0.1:6060".
// It returns the server instance so callers can shut it down when done.
func StartPprofServer(addr string) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("pprof server error: %v", err)
		}
	}()

	return srv, nil
}

// StopPprofServer gracefully shuts down the provided pprof HTTP server.
func StopPprofServer(ctx context.Context, srv *http.Server) error {
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}
