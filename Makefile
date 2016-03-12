vendoring:
	export GO15VENDOREXPERIMENT=1

build: vendoring
	go build

savedeps: vendoring
	godep save ./...

test: vendoring
	go test -v ./beat 

