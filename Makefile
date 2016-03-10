BEATNAME=amqpbeat
SYSTEM_TESTS=false
TEST_ENVIRONMENT=false

.PHONY: with_pfring
with_pfring:
	go build --tags havepfring


# This is called by the beats-packer to obtain the configuration file
.PHONY: install-cfg
install-cfg:
	cp etc/amqpbeat.template.json $(PREFIX)/amqpbeat.template.json
	# linux
	cp amqpbeat.yml $(PREFIX)/amqpbeat-linux.yml
	# binary
	cp amqpbeat.yml $(PREFIX)/amqpbeat-binary.yml
	# darwin
	cp amqpbeat.yml $(PREFIX)/amqbpeat-darwin.yml
	sed -i.bk 's/device: any/device: en0/' $(PREFIX)/amqpbeat-darwin.yml
	rm $(PREFIX)/amqpbeat-darwin.yml.bk
	# win
	cp amqpbeat.yml $(PREFIX)/amqpbeat-win.yml
	sed -i.bk 's/device: any/device: 0/' $(PREFIX)/amqpbeat-win.yml
	rm $(PREFIX)/amqpbeat-win.yml.bk



.PHONY: benchmark
benchmark:
	$(GODEP) go test -short -bench=. ./... -cpu=2

vendoring:
	export GO15VENDOREXPERIMENT=1

build: vendoring
	go build

savedeps: vendoring
	godep save ./...

test: vendoring
	go test -v ./beat 

