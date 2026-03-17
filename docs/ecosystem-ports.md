# Ecosystem Port Configuration

Standard port assignments and configuration for all services in the ecosystem.

## Quick Reference

| Service | Port | Protocol | Purpose | Env Override |
|---------|------|----------|---------|--------------|
| Ratatosk | 6379 | TCP (RESP3) | Redis-compatible data store (default) | `RATATOSK_PORT` |
| Ratatosk | 6380 | TCP (RESP3) | Recommended coexistence port | `RATATOSK_PORT` |
| Muninn | 6333 | HTTP | REST API (axum) | `MUNINN_PORT` |
| Muninn | 6334 | gRPC | gRPC API (tonic) | `MUNINN_GRPC_PORT` |
| Conductor | 9100 | TCP (JSON-RPC) | Command-center bridge | `CONDUCTOR_COMMAND_CENTER_ADDR` |
| Conductor | 8090 | HTTP | Planner (FastAPI/uvicorn) | `CONDUCTOR_PLANNER_URL` |
| Ironclaw | 8080 | HTTP/WebSocket | Gateway | `IRONCLAW_GATEWAY_PORT` |

## Ratatosk

RESP3 in-memory data store serving as cache and Pub/Sub event bus.

| Property | Value |
|----------|-------|
| Default port | `6379` |
| Recommended coexistence port | `6380` (avoids collision with co-located Redis) |
| Port env var | `RATATOSK_PORT` |
| Bind address | `127.0.0.1` |
| Bind env var | `RATATOSK_BIND` |
| Protocol | TCP (RESP2/RESP3) |
| TLS | Not built-in; proxy-layer termination recommended (stunnel, nginx stream, envoy) |

A startup warning is emitted when using port 6379. The systemd autostart unit defaults to 6380.

## Muninn

Vector database with REST and gRPC interfaces for semantic search and memory.

### REST API

| Property | Value |
|----------|-------|
| Default port | `6333` |
| Port env var | `MUNINN_PORT` |
| Bind address | `127.0.0.1` |
| Bind env var | `MUNINN_HOST` |
| Protocol | HTTP (axum) |
| TLS | Not built-in; non-loopback bind requires `MUNINN_ALLOW_INSECURE_BIND=true` or TLS proxy |

### gRPC API

| Property | Value |
|----------|-------|
| Default port | `6334` |
| Port env var | `MUNINN_GRPC_PORT` |
| Bind address | `127.0.0.1` (shares `MUNINN_HOST`) |
| Protocol | gRPC (tonic) |
| TLS | Not built-in; same insecure-bind guard as REST |

## Conductor

Workflow execution engine with a JSON-RPC bridge and HTTP planner.

### Command-center bridge (TCP)

| Property | Value |
|----------|-------|
| Default address | `127.0.0.1:9100` |
| Env var | `CONDUCTOR_COMMAND_CENTER_ADDR` (full `host:port`) |
| Protocol | TCP line-delimited JSON-RPC |
| TLS | Not built-in |

### Platform API

| Property | Value |
|----------|-------|
| Default address | `127.0.0.1:9150` |
| Env var | `CONDUCTOR_PLATFORM_API_ADDR` (full `host:port`) |
| Protocol | TCP JSON-RPC |
| TLS | Not built-in |

### Planner (HTTP)

| Property | Value |
|----------|-------|
| Default URL | `http://127.0.0.1:8090` |
| Env var | `CONDUCTOR_PLANNER_URL` (full URL) |
| Protocol | HTTP (FastAPI/uvicorn) |
| TLS | Required for non-local hosts (enforced by `PlannerEndpoint`) |

## Ironclaw

AI agent gateway serving HTTP and WebSocket connections.

| Property | Value |
|----------|-------|
| Default port | `8080` |
| Port env var | `IRONCLAW_GATEWAY_PORT` |
| Bind address | `127.0.0.1` (loopback) |
| Bind env var | `IRONCLAW_GATEWAY_BIND` |
| Protocol | HTTP + WebSocket |
| TLS | Configurable via `gateway.require_tls`; proxy-layer termination supported via `trusted_proxies` |

## Standardized Naming Convention

Environment variables follow a `{SERVICE}_{COMPONENT}` pattern:

| Pattern | Examples |
|---------|----------|
| `{SERVICE}_PORT` | `RATATOSK_PORT`, `MUNINN_PORT`, `IRONCLAW_GATEWAY_PORT` |
| `{SERVICE}_BIND` | `RATATOSK_BIND`, `MUNINN_HOST`, `IRONCLAW_GATEWAY_BIND` |
| `{SERVICE}_ADDR` | `CONDUCTOR_COMMAND_CENTER_ADDR` (combined `host:port`) |
| `{SERVICE}_URL` | `CONDUCTOR_PLANNER_URL` (full URL with scheme) |

Conventions:
- Separate `_PORT` / `_BIND` variables when the service uses a simple TCP/HTTP listener.
- Combined `_ADDR` (`host:port`) when the variable configures a connection target rather than a listener.
- Full `_URL` (with scheme) when the protocol may vary (HTTP vs HTTPS).
- All services default to loopback (`127.0.0.1`) and require explicit opt-in for non-loopback binding.

## Cross-Service Dependencies

| Consumer | Dependency | Default Target | Env Override (consumer side) |
|----------|------------|----------------|------------------------------|
| Ironclaw | Ratatosk (cache) | `redis://127.0.0.1/` | `IRONCLAW_STORAGE_REDIS_URL` |
| Ironclaw | Muninn (memory) | `http://127.0.0.1:8000` | `IRONCLAW_STORAGE_MUNINN_URL` |
| Ironclaw | Conductor (bridge) | env-only, no default | `IRONCLAW_CONDUCTOR_ADDR` |
| Conductor | Muninn | env-only | `CONDUCTOR_MUNINN_ADDR` |
| Conductor | Ratatosk | via planner/runtime | `CONDUCTOR_PLANNER_URL` |
