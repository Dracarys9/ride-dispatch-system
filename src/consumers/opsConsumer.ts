import "dotenv/config";
import { createRedisClient, RIDE_EVENTS_CHANNEL } from "../config/redis";
import { RideEvent, RideStatus } from "../types/ride";

const subscriber = createRedisClient("ops-sub");
const statusCounts: Record<RideStatus, number> = {
  REQUESTED: 0,
  ASSIGNED: 0,
  NO_DRIVER_FOUND: 0,
};
let totalEvents = 0;

function printSummary(prefix: string): void {
  console.log(
    `[OPS] ${prefix} (${totalEvents} events) -> REQUESTED: ${statusCounts.REQUESTED} | ASSIGNED: ${statusCounts.ASSIGNED} | NO_DRIVER_FOUND: ${statusCounts.NO_DRIVER_FOUND}`,
  );
}

function handleMessage(_channel: string, message: string): void {
  let event: RideEvent;
  try {
    event = JSON.parse(message) as RideEvent;
  } catch (error) {
    console.error("[OPS] Failed to parse ride event", error);
    return;
  }

  statusCounts[event.status]++;
  totalEvents++;
  console.log(
    `[OPS] ride ${event.rideId} is now in status ${event.status}${event.driverId ? ` (driver: ${event.driverId})` : ""} @ ${event.timestamp}`,
  );

  if (totalEvents % 25 === 0) {
    printSummary("Running Summary");
  }
}

subscriber.on("message", (channel, message) => {
  handleMessage(channel, message);
});

subscriber
  .subscribe(RIDE_EVENTS_CHANNEL)
  .then(() => {
    console.log(`[OPS] Monitoring all ride events on ${RIDE_EVENTS_CHANNEL}...`);
  })
  .catch((error) => {
    console.error("[OPS] Failed to subscribe to ride events", error);
    process.exit(1);
  });

let isShuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;
  printSummary("Final Summary");
  console.log(`[OPS] Received ${signal}, shutting down`);

  await subscriber.unsubscribe(RIDE_EVENTS_CHANNEL);
  await subscriber.quit();
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void shutdown(signal)
      .then(() => process.exit(0))
      .catch((error) => {
        console.error("[OPS] Shutdown failed", error);
        process.exit(1);
      });
  });
}