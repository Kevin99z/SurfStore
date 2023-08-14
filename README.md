# Surfstore

## Overview

Surfstore is a fault-tolerant distributed storage service that leverages [the Raft consensus algorithm](https://raft.github.io). It consists of multiple Raft Servers that communicate with each other via GRPC to ensure consistency and reliability.

## Features

- Fault Tolerance: The system is resilient to crashes and can continue to function as long as a majority of the nodes are operational.
- Concurrent Client Access: Multiple clients can connect to the SurfStore service simultaneously.
- Consistent File View: Clients see a consistent set of updates to files.
- Error Handling: Clients interacting with a non-leader will receive an error message and must retry to find the leader.
- Scalability: In a real deployment, Surfstore can handle exabytes of data, requiring tens of thousands of BlockStores or more.


## Design

### Server
Each Surfstore Server is composed of two main services:

- BlockStore: Manages the content of each file, which is divided into chunks or blocks. Each block has a unique identifier, and this service stores and retrieves these blocks.
- MetaStore: Manages the metadata of files and the entire system. It holds the mapping of filenames to blocks and is aware of available BlockStores, mapping blocks to particular BlockStores.


### Communication
Servers communicate with each other using GRPC. The leader is responsible for querying a majority quorum of the nodes and responding to client requests.
Each server is aware of all other possible servers (from the configuration file), and new servers do not dynamically join the cluster.
Using the protocol, if the leader can query a majority quorum of the nodes, it will reply back to the client with the correct answer. As long as a majority of the nodes are up and not in a crashed state, the clients should be able to interact with the system successfully. 
When a majority of nodes are in a crashed state, clients should block and not receive a response until a majority are restored. Any clients that interact with a non-leader should get an error message and retry to find the leader.



## Usage

Run BlockStore server:
```console
$ make run-blockstore
```

Run RaftSurfstore server:
```console
$ make IDX=0 run-raft
```

Test:
```console
$ make test
```

Specific Test:
```console
$ make TEST_REGEX=Test specific-test
```

Clean:
```console
$ make clean
```
