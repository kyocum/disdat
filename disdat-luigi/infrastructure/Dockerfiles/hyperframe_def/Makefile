all: protoc hframe

bundle:
	@docker run  -v $(CURDIR):$(CURDIR) -w $(CURDIR) protoc3.2.0 bundle.proto --python_out=. -I.

hframe:
	@docker run  -v $(CURDIR):$(CURDIR) -w $(CURDIR) protoc3.2.0 hyperframe.proto --python_out=. -I.

protoc:
	@docker build -t protoc3.2.0 .
