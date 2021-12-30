# AlmaLinux uses older glibc 2.28
# https://stackoverflow.com/questions/55450061/go-build-with-another-glibc
# https://medium.com/@diogok/on-golang-static-binaries-cross-compiling-and-plugins-1aed33499671

# https://pkg.go.dev/cmd/cgo
knit-bigquery-module: go.mod go.sum main.go
	CGO_ENABLED=0 go build -ldflags '-s -w'
