# MaRedis: A Redis Clone in Go

**MaRedis** is a Redis clone implemented in Go for educational purposes. It provides a foundational understanding of Redis's core concepts and architecture.

## Key Features:

- **Network Server:** Establishes a TCP server to handle client connections using Redis's RESP protocol.
- **Basic Commands:** Supports fundamental Redis commands like (SET, GET, HSET, HGET and DELETE) for basic key-value operations.
- **Persistence:** Implements the Append-Only File (AOF) persistence mechanism to ensure data durability.

## Project Structure:

- **Sockets:** Handles network communication and client connections.
- **RESP:** Implements the Redis Serialization Protocol (RESP) for efficient data transfer.
- **AOF:** Manages the Append-Only File for data persistence.
- **Database:** Stores in-memory key-value data.

## Getting Started:

just go the repo path and run to build your executable file:
```bash
$ go build maredis .
```

## Future Enhancements:

Implement additional Redis commands (e.g., EXISTS, INCR, EXPIRE).
Explore other persistence mechanisms (e.g., RDB).
Enhance performance and scalability.
