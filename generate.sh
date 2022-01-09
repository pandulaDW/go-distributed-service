# generate protoc code
protoc --go_out=. --go-grpc_out=. api/v1/log.proto

# create config path to store the certificates
CONFIG_PATH=${HOME}/.go_distributed_service/
mkdir -p ${CONFIG_PATH}

# generate CA certs
cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca

# generate certs for the server using the CA keys
cfssl gencert \
                -ca=ca.pem \
                -ca-key=ca-key.pem \
                -config=test/ca-config.json \
                -profile=server \
                test/server-csr.json | cfssljson -bare server

# move the keys to the designated location
mv *.pem *.csr ${CONFIG_PATH}
