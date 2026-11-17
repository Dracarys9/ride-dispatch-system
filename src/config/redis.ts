import "dotenv/config";
import Redis from "ioredis";

export const REDIS_CONFIG = {
  host: process.env.REDIS_HOST ?? "localhost",
  port: Number(process.env.REDIS_PORT ?? 6379),
};

export const DISPATCH_QUEUE_NAME = "ride-dispatch-queue";
export const RIDE_EVENTS_CHANNEL = "ride-events-channel";

export function createRedisClient(role = "default"): Redis {
  const client = new Redis({
    ...REDIS_CONFIG,
    maxRetriesPerRequest: null,
  });

  client.on("connect", () => {
    console.log(`[Redis:${role}] Connected`);
  });

  client.on("error", (error: Error) => {
    console.error(`[Redis:${role}] Error`, error);
  });

  return client;
}