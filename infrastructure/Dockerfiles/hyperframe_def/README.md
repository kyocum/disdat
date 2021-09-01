Requirements
------------
Docker

Instructions
------------
1. Build the protobuf 3.2.0 docker image

```bash
make protoc
```

2. Build the protobuf bundle message library

```bash
make bundle
```

3. Alternatively, build everything at once using

```bash
make
```