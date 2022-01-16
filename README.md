# Distributed commit log service 

A distributed commit log library built using Go, protobufs and gRPC. The code was written by following the book [distributed services with go](https://pragprog.com/titles/tjgo/distributed-services-with-go).  

## The Log Library
- Logs are written as binary data after serializing using protobuf format. 
- The Log library consists of several abstractions. At the lowest level, the logs are persisted in files (store file) using a binary format.
- Index files are created where an index entry is created for each log added. The index files are memory-mapped for fast reading.
- Each store and index file combination is wrapped in a Segment, where old segments are deleted and an active segment is maintained for writing.
- A primary abstraction called Log is maintained around the segments.

## Networking
- gRPC is used for handling rpc calls between the internal services.
- The application includes 4 different types of handlers.
  - Produce handler for producing a log.
  - Consume handler for consuming a log.
  - Server side streaming handler to read all logs after a given offset.
  - Bidirectional streaming handler so the client can stream data into the server’s
    log and produce and consume logs in any desired pattern quickly.
  - A bulk stream request handler to insert large number of records quickly with less network calls.  

## Security
- A PKI is implemented using the [CFSSL](https://github.com/cloudflare/cfssl) library and its CLI was used to generate test certficates.
- Connections are encrypted with TLS, through mutual TLS authentication to verify the identities of clients. 
- Authentication is done using using ACL-based authorization to permit client actions using the [Casbin](https://github.com/casbin/casbin) library.