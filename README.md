# Sahibinden Case: Distributed Cache Client

This project demonstrates a case implementation for Sahibinden.com's technical assessment. It showcases an approach to building a distributed cache system using ZooKeeper for coordination.

## Project Structure

The project consists of several modules:

- **cache-client**: A Java library that provides cache client functionality
- **cache-server**: An application that serves as the cache server with REST API endpoints
- **sample**: A sample application demonstrating how to use the cache client

## Features

- Distributed cache implementation using ZooKeeper for coordination
- REST API for cache operations (GET, PUT/POST, DELETE)
- In-memory cache with ZooKeeper-based coordination
- Dockerized deployment with docker-compose

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

The project includes a docker-compose.yml file for easy deployment:

```bash
docker-compose up
```

This will start:
- 10 cache-server instances
- A ZooKeeper instance
- A sample client application

## Cache Server API

The cache server exposes the following endpoints:
- GET `/cache/{key}` - Retrieve a value
- PUT `/cache/{key}` - Store a value
- DELETE `/cache/{key}` - Delete a value

## Configuration

The cache server can be configured via:
- System properties: `zookeeper.connect`
- Environment variables: `ZOOKEEPER_CONNECT` or `ZK_CONNECT`
- Defaults to `localhost:2181`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.