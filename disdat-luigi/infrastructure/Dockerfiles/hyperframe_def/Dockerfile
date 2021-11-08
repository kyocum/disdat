FROM ubuntu:16.04
ADD https://github.com/google/protobuf/releases/download/v3.2.0/protoc-3.2.0-linux-x86_64.zip /protobuf/
RUN apt-get -y update && apt-get install -y unzip
RUN cd protobuf && unzip protoc-3.2.0-linux-x86_64.zip
ENTRYPOINT ["/protobuf/bin/protoc", "-I/protobuf"]
CMD []