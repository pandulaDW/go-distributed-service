# Distributed commit log service 

A distributed commit log library built using Go, protobufs and gRPC. The code was written by following the book [distributed services with go](https://pragprog.com/titles/tjgo/distributed-services-with-go).  

## The Log Library
- Logs are written as binary data after serializing using protobuf format. 
- The Log library consists of several abstractions. At the lowest level, the logs are persisted in files (store file) using a binary format.
- Index files are created where an index entry is created for each log added. The index files are memory-mapped for fast reading.
- Each store and index file combination is wrapped in a Segment, where old segments are deleted and an active segment is maintained for writing.
- A primary abstraction called Log is maintained around the segments.

## Features of the service
- The connections are made secure by authenticating the server with SSL/TLS  and by authenticating requests with access tokens.
- The service has been made observable by adding logs, metrics, and tracing.
- Server-to-Server service discovery is made by making server instances aware of each other.
- Consensus is added to coordinate the efforts of the servers and turn them into a cluster.
- Discovery in the gRPC clients is added so they discover and connect to the servers with client-side load balancing