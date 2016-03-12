export GO15VENDOREXPERIMENT=1
export version=0.9.0.alpha
export beatname=amqpbeat
export buildname=$(beatname)-$(version)

amqpbeat: 
	go build

savedeps:
	godep save ./...

test:
	go test -v ./beat 

build-god: 
	# This only works on linux
	# TODO: create docker file for builds
	rm -rf work
	mkdir work
	git clone git@github.com:robinpercy/go-daemon.git work/go-daemon
	cd work/go-daemon && make
	cp work/go-daemon/god ./$(beatname)-god

dist: $(beatname) $(beatname)-god
	rm -rf dist
	mkdir -p "dist/$(buildname)"
	cp $(beatname) "dist/$(buildname)/"
	cp $(beatname)-god "dist/$(buildname)/"
	cd dist && tar -czvf "$(buildname).tar.gz" "$(buildname)"
	mv dist/"$(buildname).tar.gz" ./

clean:
	rm -rf work
	rm -rf dist

