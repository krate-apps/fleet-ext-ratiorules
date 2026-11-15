.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/ratiorules-worker ./cmd/ratiorules-worker

pack:
	bash scripts/pack.sh dist
