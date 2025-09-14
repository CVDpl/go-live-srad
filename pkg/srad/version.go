package srad

// Version is the semantic version of the srad library.
// It can be overridden at build time using:
//
//	go build -ldflags "-X github.com/CVDpl/go-live-srad/pkg/srad.Version=1.0.3"
//
// or:
//
//	go test -ldflags "-X github.com/CVDpl/go-live-srad/pkg/srad.Version=1.0.3" ./...
//
// Default value follows SemVer.
var Version = "1.0.5"
