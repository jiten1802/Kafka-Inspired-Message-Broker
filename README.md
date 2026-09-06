# Kafka-Inspired Message Broker

A lightweight C++ TCP server that implements a subset of the Kafka protocol and serves as a toy Kafka broker for API discovery, topic metadata lookup, and fetch requests. The project is built around socket handling, Kafka wire-format parsing, and metadata-backed topic/partition reads from the KRaft log directory.

This repository was created as part of the CodeCrafters "Build Your Own Kafka" challenge, but it is organized as a practical broker implementation with clear request dispatch, metadata parsing, and response serialization.

## Overview

The server:

- listens on port 9092 for incoming Kafka clients
- accepts concurrent client connections using std::jthread
- parses Kafka request headers and request bodies from raw TCP buffers
- responds to supported Kafka APIs:
  - ApiVersions
  - DescribeTopicPartitions
  - Fetch
- reads cluster metadata from the Kafka metadata log in /tmp/kraft-combined-logs
- resolves topic UUIDs and partition data from metadata files
- serializes request/response messages using custom Kafka wire-protocol helpers

## Features

### Supported Kafka APIs

- ApiVersionsRequest
  - returns supported broker API versions
  - advertises the supported key set for the broker

- DescribeTopicPartitionsRequest
  - looks up the requested topic name
  - returns partition metadata including leader, replicas, ISR, and topic UUID
  - returns UNKNOWN_TOPIC_OR_PARTITION when the topic is missing

- FetchRequest
  - resolves the topic UUID to a partition log file
  - reads records from the partition log file
  - returns a serialized FetchResponse with one or more record batches

### Internal capabilities

- custom binary parsing for Kafka primitive types, compact arrays, variable ints, tagged fields, and nullable strings
- minimal socket abstraction for bind/listen/accept/send/recv
- metadata parsing for Kafka KRaft-style topic and partition records
- logging and hex/dump utilities for debugging binary wire data

## Project structure

```text
.
├── CMakeLists.txt
├── README.md
├── codecrafters.yml
├── LICENSE
├── vcpkg.json
├── your_program.sh
└── src/
    ├── ClusterMetadata.cc
    ├── ClusterMetadata.h
    ├── KafkaApis.cc
    ├── KafkaApis.h
    ├── main.cpp
    ├── MessageDefs.cc
    ├── MessageDefs.h
    ├── TCPManager.cc
    ├── TCPManager.h
    ├── Utils.cc
    └── Utils.h
```

## Build and run

### Prerequisites

- CMake 3.13+
- C++23 compatible compiler
- Linux/macOS environment with socket APIs available
- optional: vcpkg setup if your environment expects it

### Local build

```bash
cmake -B build -S .
cmake --build ./build
```

### Run the broker

```bash
./your_program.sh
```

This script configures the build and launches the compiled binary `build/kafka`.

### Default listening port

The broker binds to:

```text
0.0.0.0:9092
```

## Runtime behavior

When the server starts:

1. it creates a socket and listens on port 9092
2. it accepts incoming client connections
3. each client gets its own std::jthread worker
4. the worker repeatedly reads Kafka request payloads from the socket
5. the request is classified using the Kafka API key in the request header
6. a matching response is encoded and written back to the client

The program also installs a SIGINT handler so it can shut down gracefully when interrupted.

## Architecture

### 1. main.cpp

`src/main.cpp` is the entry point.

It:

- initializes a `TCPManager`
- starts listening
- creates a `ClusterMetadata` instance
- accepts connections in a loop
- creates one thread per client
- dispatches data read from each client through `KafkaApis`

### 2. TCPManager

`src/TCPManager.h` and `src/TCPManager.cc` provide a thin socket wrapper.

Responsibilities:

- create socket and bind to port 9092
- set SO_REUSEADDR for restart stability
- accept client connections
- send responses with broker-encoded Kafka messages
- read incoming buffers from client sockets

### 3. KafkaApis

`src/KafkaApis.h` and `src/KafkaApis.cc` are the protocol request handlers.

This component:

- decodes request headers via `RequestHeader::fromBuffer`
- switches on the API key
- invokes the correct handler for:
  - API_VERSIONS
  - DESCRIBE_TOPIC_PARTITIONS
  - FETCH
- writes the response back to the client using the TCP manager

### 4. ClusterMetadata

`src/ClusterMetadata.h` and `src/ClusterMetadata.cc` parse Kafka cluster metadata from the log directory.

It reads metadata records from:

```text
/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
```

Then it builds:

- `topic_name_uuid_map`: topic name -> UUID
- `topic_uuid_name_map`: UUID -> topic name
- `topic_uuid_partition_id_map`: UUID -> list of partition records

This metadata is later used when serving fetch and partition metadata requests.

### 5. MessageDefs and Utils

These are the serialization and deserialization layers.

`src/MessageDefs.h` defines Kafka request/response message structs such as:

- `RequestHeader`
- `ApiVersionsRequestMessage`
- `DescribeTopicPartitionsRequest`
- `FetchRequest`
- `ApiVersionsResponseMessage`
- `DescribeTopicPartitionsResponse`
- `FetchResponse`

`src/Utils.h` and `src/Utils.cc` define reusable helpers for:

- variable-length integers
- nullable strings
- compact arrays
- tagged fields
- endian conversion
- byte hex dumping

These utilities are essential because Kafka protocol encoding is binary-heavy and strict about field ordering and sizes.

## Request flow examples

### ApiVersions request

When a client sends an `ApiVersions` request, the broker:

- parses the request header
- reads the request API version
- validates whether it is supported
- builds an `ApiVersionsResponseMessage`
- includes the supported API keys and version ranges
- writes the response on the client socket

### DescribeTopicPartitions request

This path:

- reads the request topic list
- checks `cluster_metadata.topic_name_uuid_map`
- returns metadata for each partition in the topic
- includes:
  - partition ID
  - leader ID
  - leader epoch
  - replica arrays
  - ISR arrays
  - topic UUID

### Fetch request

This is the most involved path:

- the fetch request contains one or more topics and partition fetch instructions
- the code finds the corresponding topic UUID in the cluster metadata
- it reads the partition log file from the Kafka data directory
- it creates a `FetchResponse` and appends the raw record batch data
- the result is sent back to the client

## Binary protocol notes

The project intentionally implements low-level Kafka wire-format handling rather than using higher-level libraries. That means you will see custom logic for:

- network byte order conversion
- variable integer encodings
- compact array length prefixes
- nullable strings
- binary record batches and metadata records

This is a faithful challenge-style approach to Kafka protocol decoding.

## Data files used at runtime

The broker assumes Kafka-style metadata and partition log files exist under `/tmp/kraft-combined-logs`.

Typical paths look like:

```text
/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
/tmp/kraft-combined-logs/<topic-name>-<partition-id>/00000000000000000000.log
```

The implementation waits for the metadata file to appear before reading it, and it uses it to populate the in-memory cluster view.

## Known implementation notes

This project is intentionally a compact subset of Kafka, not a full production broker. It focuses on protocol interaction and binary message handling rather than:

- replication coordination
- partition leader election
- consumer groups
- offset management
- full storage semantics
- authentication and authorization

## Requirements and limitations

### Supported behavior

- single-node toy Kafka-style broker
- request parsing for a subset of Kafka protocol APIs
- basic response generation for metadata and fetch operations
- metadata-driven topic/partition discovery

### Not a full Kafka implementation

This is not a drop-in replacement for Kafka broker software. It does not implement the full production protocol suite, clustering model, or storage semantics expected by a real Kafka deployment.

## Building for CodeCrafters

This repository is configured for CodeCrafters challenge execution via:

- `codecrafters.yml`
- `your_program.sh`

The challenge configuration uses C++23 and expects the compiled binary to be called `kafka`.

## Example command sequence

```bash
cmake -B build -S .
cmake --build ./build
./build/kafka
```

If you are running this via the project helper script:

```bash
./your_program.sh
```

## License

This project is distributed under the MIT license. See [LICENSE](LICENSE) for details.

## Summary

This repository is a small, protocol-aware Kafka-inspired broker that demonstrates how to:

- accept client socket connections
- parse Kafka request headers and payloads
- decode metadata records
- serve topic metadata and fetch responses
- work with raw binary Kafka wire data in C++

It is best understood as an educational implementation for learning the Kafka protocol, not as a production-ready message broker.
