package srad

// Version is the semantic version of the srad library.
// It can be overridden at build time using:
//
//	go build -ldflags "-X github.com/CVDpl/go-live-srad/pkg/srad.Version=1.1.2"
//
// or:
//
//	go test -ldflags "-X github.com/CVDpl/go-live-srad/pkg/srad.Version=1.1.2" ./...
//
// Default value follows SemVer.
var Version = "1.1.2"
