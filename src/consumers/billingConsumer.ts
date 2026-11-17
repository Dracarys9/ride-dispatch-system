import "dotenv/config";
import { createRedisClient, RIDE_EVENTS_CHANNEL } from "../config/redis";
import { RideEvent } from "../types/ride";

const subscriber = createRedisClient("billing-sub");
const stateClient = createRedisClient("billing-state");
const PROCESSED_EVENTS_KEY = "billing:processed_events";

export async function markEventProcessed(eventId: string): Promise<boolean> {
  const added = await stateClient.sadd(PROCESSED_EVENTS_KEY, eventId);
  return added === 1;
}

async function handleMessage(_channel: string, message: string): Promise<void> {
  let event: RideEvent;
  try {
    event = JSON.parse(message) as RideEvent;
  } catch (error) {
    console.error("[BILLING] Failed to parse ride event", error);
    return;
  }

  try {
    if (!(await markEventProcessed(event.eventId))) {
      console.log(`[BILLING] Duplicate event skipped: ${event.eventId}`);
      return;
    }

    if (event.status === "ASSIGNED") {
      console.log(
        `[BILLING] charging rider for ride ${event.rideId} (Driver: ${event.driverId}, Event: ${event.eventId})`,
      );
      return;
    }

    console.log(
      `[BILLING] observed ${event.status} for ride ${event.rideId} — no charge triggered`,
    );
  } catch (error) {
    console.error(`[BILLING] Failed to process event ${event.eventId}`, error);
  }
}

subscriber.on("message", (channel, message) => {
  void handleMessage(channel, message);
});

subscriber
  .subscribe(RIDE_EVENTS_CHANNEL)
  .then(() => {
    console.log(`[BILLING] Listening for ride events on ${RIDE_EVENTS_CHANNEL}...`);
  })
  .catch((error) => {
    console.error("[BILLING] Failed to subscribe to ride events", error);
    process.exit(1);
  });

let isShuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;
  console.log(`[BILLING] Received ${signal}, shutting down`);

  await subscriber.unsubscribe(RIDE_EVENTS_CHANNEL);
  await subscriber.quit();
  await stateClient.quit();
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void shutdown(signal)
      .then(() => process.exit(0))
      .catch((error) => {
        console.error("[BILLING] Shutdown failed", error);
        process.exit(1);
      });
  });
}