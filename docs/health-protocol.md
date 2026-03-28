# Ecosystem Health Protocol

Standard health response format used across all components.

## Wire Format

```json
{
  "status": "healthy",
  "timestamp_unix_s": 1710518400,
  "version": "0.1.0",
  "bridge_contract_version": "0.1",
  "components": [
    {
      "name": "storage",
      "status": "ok",
      "message": null
    },
    {
      "name": "cache",
      "status": "degraded",
      "message": "circuit breaker open"
    }
  ]
}
```

## Status Values

### Top-level `status`

| Value | Meaning | HTTP Code |
|-------|---------|-----------|
| `healthy` | All components operational | 200 |
| `degraded` | Some components impaired, core function continues | 200 |
| `unhealthy` | Critical failure, not serving traffic | 503 |

### Component `status`

| Value | Meaning |
|-------|---------|
| `ok` | Fully operational |
| `degraded` | Impaired but functional |
| `failed` | Not operational |
| `unknown` | Status cannot be determined |
| `noop` | Using noop/stub implementation |

## Aggregation Rule

- Any component `failed` -> top-level `unhealthy`
- Any component `degraded` or `noop` -> top-level `degraded`
- All components `ok` -> top-level `healthy`

## Per-Component Implementation

### Ratatosk

- **Endpoint**: `INFO server` section (field: `health_status`)
- **Components**: persistence (AOF latch + last RDB/AOF rewrite status + audit checkpoint state), memory (`maxmemory` headroom when configured)
- **Contract version**: Exposed in `INFO server` as `bridge_contract_version`

Current Ratatosk mapping:
- `healthy`: persistence status is OK, audit chain is not dirty, and memory headroom is within configured `maxmemory` budget
- `degraded`: last RDB/AOF rewrite status is error, audit checkpoint is dirty, or storage headroom is low
- `unhealthy`: AOF write is latched or cached memory estimate exceeds configured `maxmemory`

Ratatosk persistence/health surfaces also expose:
- `INFO persistence`: `audit_chain_dirty`, `audit_recovery_status`
- `PING HEALTH`: `status`, `audit_chain_dirty`, `audit_recovery_status`

### Muninn

- **Endpoint**: `GET /health`
- **Components**: inference (engine health), disk (space check), storage (write probe), recovery (WAL replay status)
- **Contract version**: Field in health JSON response

### Conductor

- **Endpoint**: Bridge health handler (JSON-RPC `health` method)
- **Components**: planner (HTTP ready check), sqlite (ping), memory (adapter health), cache (circuit breaker state), inference_pool (if configured)
- **Contract version**: Field in health JSON response

### Ironclaw

- **Endpoint**: `health` RPC method
- **Components**: storage (session backend), channels (per-channel connected status), memory (Muninn gateway), agent (LLM provider), mcp (server statuses)
- **Contract version**: Field in health JSON response

## Probing

Upstream services should:
1. Check `status` field first (fast path)
2. Inspect `components` only when `status != "healthy"`
3. Use `bridge_contract_version` to detect version skew
4. Treat missing fields as `unknown` (forward-compatible)

## Timeouts

- Health probes: 2 second timeout
- Dependency probes within health: 1 second each
- Disk probes: 500ms
