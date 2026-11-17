import { v4 as uuidv4 } from "uuid";
import {
  createRedisClient,
  RIDE_EVENTS_CHANNEL,
} from "../config/redis";
import { RideEvent, RideStatus } from "../types/ride";

const redis = createRedisClient("publisher");

export async function publishRideEvent(input: {
  rideId: string;
  status: RideStatus;
  driverId: string | null;
}): Promise<RideEvent> {
  const event: RideEvent = {
    eventId: uuidv4(),
    rideId: input.rideId,
    status: input.status,
    driverId: input.driverId,
    timestamp: new Date().toISOString(),
  };

  await redis.publish(RIDE_EVENTS_CHANNEL, JSON.stringify(event));
  console.log(
    `[EVENT] Published ${event.status} for ${event.rideId} (eventId: ${event.eventId})`,
  );
  return event;
}

export async function closePublisherRedis(): Promise<void> {
  await redis.quit();
}