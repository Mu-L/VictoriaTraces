# All these commands must run from repository root.

gen-otel-proto:
	$(DOCKER_RUN) --volume "$(shell pwd)/lib/proto:/workspace" --workdir /workspace bufbuild/buf generate
