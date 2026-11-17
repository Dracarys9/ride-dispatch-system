# Ride Booking & Asynchronous Dispatch System

A production-inspired, distributed ride-hailing dispatch engine built with **Node.js**, **TypeScript**, **Express**, **BullMQ**, and **Redis**. The system decouples non-blocking HTTP ride ingestion from concurrent background driver matching, enforces atomic distributed driver locks, and broadcasts state transitions via Redis Pub/Sub to independent downstream consumers.

## System Architecture

```text
┌───────────────────┐     1. POST /rides (202 Accepted)      ┌───────────────────┐
│   Client / Load   │ ─────────────────────────────────────► │   Express API     │
│      Test         │ ◄───────────────────────────────────── │  (src/api/server) │
└───────────────────┘     Idempotency-Key / SSE Stream       └─────────┬─────────┘
                                                                       │
                      ┌────────────────────────────────────────────────┤
                      │ 2. Persist state (REQUESTED)                   │ 3. Enqueue job
                      ▼                                                ▼
            ┌───────────────────┐                            ┌───────────────────┐
            │    Redis Store    │                            │   BullMQ Queue    │
            │  (ride:<rideId>)  │                            │ (dispatch-queue)  │
            └─────────▲─────────┘                            └─────────┬─────────┘
                      │                                                │
                      │ 4. Atomic SET NX lock +                        │
                      │    Lua release & state update                  ▼
            ┌─────────┴────────────────────────────────────────────────┴─────────┐
            │                 Concurrent Dispatch Worker Pool (x10)              │
            │                   (src/worker/dispatchWorker.ts)                   │
            └─────────────────────────────────┬──────────────────────────────────┘
                                              │
                                              │ 5. Publish RideEvent
                                              ▼
                               ┌─────────────────────────────┐
                               │ Redis Pub/Sub Fan-Out Bus   │
                               │   (ride-events-channel)     │
                               └──────────────┬──────────────┘
                                              │
                     ┌────────────────────────┴────────────────────────┐
                     ▼ (100% Broadcast)                                ▼ (100% Broadcast)
      ┌──────────────────────────────┐                  ┌──────────────────────────────┐
      │ Program A: Billing Consumer  │                  │ Program B: Ops Monitor       │
      │ (Idempotent charge on        │                  │ (Live state logging &        │
      │  ASSIGNED via Redis SET)     │                  │  running system tallies)     │
      └──────────────────────────────┘                  └──────────────────────────────┘
```

---

## Core & Production Features

1. **Non-Blocking Asynchronous Ingestion (`POST /rides` → `202 Accepted`):**
   * Validates incoming rider requests via `zod`, saves initial ride state (`REQUESTED`) to Redis, pushes a `{ rideId }` job onto BullMQ, broadcasts a `REQUESTED` event, and immediately returns `202 Accepted` without blocking the HTTP thread on driver negotiation.
2. **Distributed Concurrency Safety (`SET NX` + Lua Release):**
   * Across 10 concurrent worker threads competing for 10 drivers (`drv_01`–`drv_10`), workers acquire an atomic short-lived lock (`SET driver:lock:<id> <rideId> PX 3000 NX`) before offering a ride and release it inside a `try/finally` block via an atomic Lua compare-and-delete script—eliminating double-assignments under heavy load.
3. **Deterministic Retry Cap & Audit Trail:**
   * Workers track tried driver IDs per ride in a `Set` so the same driver is never pinged twice, capping rejections at **3 distinct drivers** before transitioning to `NO_DRIVER_FOUND`. Every attempt and timestamp is persisted and exposed via `GET /rides/:rideId`.
4. **True Broadcast Fan-Out & Idempotent Billing (Redis Pub/Sub):**
   * State transitions are published to `ride-events-channel` and received independently by both standalone subscribers:
     * **Billing Consumer (`billingConsumer.ts`):** Deduplicates events via `SADD billing:processed_events <eventId>` and triggers rider charges exclusively on `ASSIGNED` transitions.
     * **Ops Monitor (`opsConsumer.ts`):** Logs every status transition in real time and maintains live system counters.
5. **HTTP `Idempotency-Key` Deduplication:**
   * Passing an `Idempotency-Key` header on `POST /rides` atomically claims `idempotency:<key>` in Redis (`SET ... EX 3600 NX`). Retried requests with the same key return `200 OK` with the original `rideId` (`idempotentReplay: true`) instead of booking duplicate rides.
6. **Deterministic Chaos Testing Toggle (`?forceReject=true`):**
   * Appending `?forceReject=true` (or `X-Simulate-Reject: true`) forces all 3 drivers to reject that ride, making the 3-rejection cap and `NO_DRIVER_FOUND` fan-out deterministically verifiable on the first try.
7. **Zero-Polling Real-Time Status Stream (`GET /rides/:rideId/stream`):**
   * Opens a Server-Sent Events (`text/event-stream`) connection backed by Redis Pub/Sub that pushes live transitions (`REQUESTED` → `ASSIGNED` / `NO_DRIVER_FOUND`) to clients and automatically closes upon reaching a terminal state.

---

## Docker Containers & Service Architecture

The stack runs as **5 isolated Docker services** orchestrated via `docker-compose.yml` with healthcheck-ordered startup:

| Container Service | Entrypoint / Image | Responsibility |
| :--- | :--- | :--- |
| **`redis`** | `redis:7-alpine` (Port `6379`) | Primary JSON ride store (`ride:<id>`), distributed lock manager (`SET NX`), BullMQ backing store, and Pub/Sub event broker. Configured with a `redis-cli ping` healthcheck. |
| **`api`** | `src/api/server.ts` (Port `3000`) | Express v5 REST & SSE server handling `POST /rides`, `GET /rides/:rideId`, `GET /rides/:rideId/stream`, and `GET /health`. |
| **`worker`** | `src/worker/dispatchWorker.ts` | BullMQ consumer (`concurrency: 10`) executing driver selection, atomic lock acquisition, offer simulation, retry caps, and terminal event publishing. |
| **`billing-consumer`** | `src/consumers/billingConsumer.ts` | Standalone Redis Pub/Sub subscriber performing event-ID deduplication and triggering rider charges on `ASSIGNED` events. |
| **`ops-consumer`** | `src/consumers/opsConsumer.ts` | Standalone Redis Pub/Sub subscriber logging every state transition (`REQUESTED`, `ASSIGNED`, `NO_DRIVER_FOUND`) and tracking running totals. |

---

## API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/rides` | Books a ride (`202 Accepted`). Supports `Idempotency-Key` header and `?forceReject=true` query param. |
| `GET` | `/rides/:rideId` | Fetches current ride state, assigned driver, and full driver attempt audit trail. |
| `GET` | `/rides/:rideId/stream` | Streams real-time ride status transitions via Server-Sent Events (SSE) until terminal state. |
| `GET` | `/health` | Returns API and Redis connectivity health status. |

---

## Live Demo Walkthrough (Feature-by-Feature Commands)

Use this sequence to demonstrate every service and feature directly from the terminal (PowerShell commands shown):

### 1. Start the 5-Container Stack & Open Live Consumer Logs
Start all containers with a clean Redis state, and open two extra terminal panes to watch both Pub/Sub consumers react simultaneously:

```powershell
# Terminal 1: Build and start all 5 services cleanly
docker compose up --build -d
docker compose exec redis redis-cli FLUSHALL
docker compose ps

# Terminal 2 (Program A — Billing Live Stream):
docker compose logs -f billing-consumer

# Terminal 3 (Program B — Ops Monitor Live Stream):
docker compose logs -f ops-consumer
```

### 2. Demonstrate Async Ride Booking, Audit Trail & Pub/Sub Fan-Out
Book a single ride (`202 Accepted` returned immediately), inspect its full driver negotiation audit trail, and observe both `billing-consumer` and `ops-consumer` receiving the exact same events in Terminals 2 and 3:

```powershell
# Book a ride asynchronously
$ride = Invoke-RestMethod -Method Post -Uri "http://localhost:3000/rides" -ContentType "application/json" -Body '{"riderId":"rider_yash","pickupLocation":"Bandra West, Mumbai","dropoffLocation":"Powai, Mumbai"}'
$ride

# Inspect full driver offer audit trail after worker negotiation (~2s)
Start-Sleep -Seconds 2
Invoke-RestMethod -Uri "http://localhost:3000/rides/$($ride.rideId)" | ConvertTo-Json -Depth 4
```

### 3. Demonstrate `Idempotency-Key` Deduplication
Send the same booking request twice with the same `Idempotency-Key` header—the second call safely returns `200 OK` with the existing `rideId` (`idempotentReplay: true`) without creating a duplicate job:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:3000/rides" -ContentType "application/json" -Headers @{ "Idempotency-Key" = "demo-idemp-001" } -Body '{"riderId":"rider_yash"}'
Invoke-RestMethod -Method Post -Uri "http://localhost:3000/rides" -ContentType "application/json" -Headers @{ "Idempotency-Key" = "demo-idemp-001" } -Body '{"riderId":"rider_yash"}'
```

### 4. Demonstrate Chaos Toggle (`?forceReject=true`) + Zero-Polling SSE Stream
Trigger 3 deterministic driver rejections and watch the live Server-Sent Events stream push `REQUESTED` and auto-close on `NO_DRIVER_FOUND`:

```powershell
$chaos = Invoke-RestMethod -Method Post -Uri "http://localhost:3000/rides?forceReject=true" -ContentType "application/json" -Body '{"riderId":"rider_chaos"}'
curl.exe -N "http://localhost:3000/rides/$($chaos.rideId)/stream"
```

### 5. Demonstrate 100-Ride Concurrency Burst & Invariant Verification
Fire 100 concurrent `POST /rides` requests simultaneously across 10 worker threads and verify zero double-assignments and zero stuck rides:

```powershell
npm run test:load
```

### 6. Tear Down Containers
```powershell
docker compose down
```

---

## Verification & Benchmark Results

### 1. 100-Ride Concurrency & Invariant Verification
Firing 100 concurrent `POST /rides` requests confirms that all rides reach a terminal state (`83 ASSIGNED + 17 NO_DRIVER_FOUND = 100`) in **~7.65s** with **0 double-assignments** and **0 stuck rides**.

![100-Ride Load Test Verification](docs/screenshots/load-test-results.png)

### 2. Independent Pub/Sub Fan-Out Proof
Both standalone subscribers receive 100% of published events independently without competing for messages:

#### Program A — Billing Consumer (Idempotent Event Processing)
Triggers rider charges exclusively on `ASSIGNED` events while safely ignoring `NO_DRIVER_FOUND` transitions.

![Billing Consumer Logs](docs/screenshots/billing-consumer-logs.png)

#### Program B — Operations Monitor (Live State & Tally Tracking)
Logs every ride