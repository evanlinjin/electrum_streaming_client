# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a streaming, sans-IO Electrum client library for Rust that provides low-level primitives and high-level clients for communicating with Electrum servers over JSON-RPC. The library supports both async (`futures`/`tokio`) and blocking transport models.

## Common Development Commands

### Build and Check
- `cargo build` - Build the project
- `cargo check` - Check if the project compiles without building
- `cargo clippy` - Run the Rust linter for code quality
- `cargo clippy --fix` - Auto-fix clippy warnings

### Testing
- `cargo test` - Run all tests
- `cargo test [TESTNAME]` - Run tests containing specific string in their names
- `cargo test --no-fail-fast` - Run all tests regardless of failures

### Documentation
- `cargo doc --open` - Build and open the documentation

## Architecture

### Core Components

1. **State Management (`src/state.rs`)**
   - Central `State<T>` struct that tracks pending requests and processes server messages
   - Maps request IDs to pending requests
   - Processes incoming notifications and responses into `Event`s

2. **Client Implementations (`src/client.rs`)**
   - `AsyncClient`: Async client for `futures`/`tokio` based transports
   - `BlockingClient`: Blocking client for synchronous operations
   - Both use channels to communicate between request sending and response handling

3. **Request/Response System**
   - `src/request.rs`: Strongly typed Electrum method wrappers
   - `src/response.rs`: Response type definitions
   - `src/batch_request.rs`: Support for batched requests
   - `src/pending_request.rs`: Tracks in-flight requests

4. **I/O Layer (`src/io.rs`)**
   - Transport-agnostic read/write utilities
   - `ReadStreamer`: Parses incoming JSON-RPC messages from a stream
   - Handles both single and batched messages

5. **Type System**
   - `MaybeBatch<T>`: Handles both single items and batches
   - `Event`: High-level events (responses, errors, notifications)
   - Custom serde implementations in `src/custom_serde.rs`

### Key Design Patterns

- **Sans-IO Core**: The `State` struct is transport-agnostic, handling protocol logic without I/O
- **Channel-based Architecture**: Clients use channels to separate request sending from response handling
- **Future-based Async**: Async requests return futures that resolve when responses arrive
- **Type Safety**: Each Electrum method has its own strongly-typed request and response structs

## Dependencies

- `bitcoin 0.32`: Bitcoin primitives and types
- `serde`/`serde_json`: JSON serialization
- `futures 0.3`: Async runtime abstractions
- `tokio` (optional): Tokio-specific async support via feature flag

## Testing Infrastructure

The project uses `bdk_testenv` for integration testing against real Electrum servers. Tests are located in `tests/synopsis.rs`.