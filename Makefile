CONFIG_PATH=${HOME}/.proglog/

.PHONY: init
init: 
	mkdir -p ${CONFIG_PATH}
	go get google.golang.org/protobuf/proto@latest
	go get google.golang.org/grpc@latest
	go get google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go get github.com/cloudflare/cfssl/cmd/cfssl@latest
	go get github.com/cloudflare/cfssl/cmd/cfssljson@latest
	go get github.com/casbin/casbin@latest
	go get go.uber.org/zap@latest
	go get go.opencensus.io@latest

.PHONY: gencert
gencert: 
	cfssl gencert \
	   -initca test/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
	   -ca=ca.pem \
	   -ca-key=ca-key.pem \
	   -config=test/ca-config.json \
	   -profile=server \
	   test/server-csr.json | cfssljson -bare server
	cfssl gencert \
	   -ca=ca.pem \
	   -ca-key=ca-key.pem \
	   -config=test/ca-config.json \
	   -profile=client \
	   -cn="normaluser" \
	   test/client-csr.json | cfssljson -bare nobody-client
	cfssl gencert \
	   -ca=ca.pem \
	   -ca-key=ca-key.pem \
	   -config=test/ca-config.json \
	   -profile=client \
	   -cn="root" \
	   test/client-csr.json | cfssljson -bare root-client

	mv *.pem *.csr ${CONFIG_PATH}
	
$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf
$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: test
test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
	go test -race ./... 
.PHONY: compile
compile: 
	protoc  api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
