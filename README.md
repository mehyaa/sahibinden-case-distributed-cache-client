# Sahibinden Case: Distributed Cache Client

This project demonstrates a case implementation for Sahibinden.com's technical assessment. It showcases an approach to building a distributed cache system using ZooKeeper for coordination.

## Project Structure

The project consists of several modules:

- **cache-server**: An application that serves as the cache server with REST API endpoints
- **cache-client**: A Java library that provides cache client functionality
- **sample**: A sample application demonstrating how to use the cache client

## Features

- REST API for cache operations (GET, PUT/POST, DELETE)
- In-memory cache with ZooKeeper-based coordination
- Distributed cache implementation using ZooKeeper for coordination
- Containerized deployment with Docker Compose

## Prerequisites

- Docker / Podman
- Java 17
- Gradle

## Building the Project

The project uses Gradle for building:

```bash
./gradlew build
```

## Running with Docker Compose

The project includes a docker-compose.yaml file for easy deployment:

```bash
docker compose up
```

This will start:
- 10 cache-server instances
- A ZooKeeper instance
- A sample client application

## Cache Server API

The cache server exposes the following endpoints:
- GET `/{key}` - Retrieve a value
- PUT `/{key}` - Store a value
- DELETE `/{key}` - Delete a value

## Configuration

The client library needs to read Zookeeper for cache servers' addresses, Zookeeper connect string can be configured via:
- System properties: `zookeeper.connect`
- Environment variables: `ZOOKEEPER_CONNECT` or `ZK_CONNECT`
- Defaults to `localhost:2181`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.