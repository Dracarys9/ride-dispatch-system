import { createRedisClient } from "../config/redis";
import { Ride } from "../types/ride";

const redis = createRedisClient("repository");

export const rideKey = (rideId: string): string => `ride:${rideId}`;
export const ALL_RIDES_SET = "rides:all";
export const driverLockKey = (driverId: string): string => `driver:lock:${driverId}`;

export async function createRide(ride: Ride): Promise<void> {
  await redis
    .multi()
    .set(rideKey(ride.rideId), JSON.stringify(ride))
    .sadd(ALL_RIDES_SET, ride.rideId)
    .exec();
}

export async function getRideById(rideId: string): Promise<Ride | null> {
  const value = await redis.get(rideKey(rideId));
  if (value === null) {
    return null;
  }

  try {
    return JSON.parse(value) as Ride;
  } catch (error) {
    console.error(`[Repository] Invalid ride data for ${rideId}`, error);
    return null;
  }
}

export async function getAllRides(): Promise<Ride[]> {
  const rideIds = await redis.smembers(ALL_RIDES_SET);
  if (rideIds.length === 0) {
    return [];
  }

  const pipeline = redis.multi();
  for (const rideId of rideIds) {
    pipeline.get(rideKey(rideId));
  }
  const results = await pipeline.exec();

  if (results === null) {
    return [];
  }

  return results.flatMap(([error, value], index) => {
    if (error || typeof value !== "string") {
      if (error) {
        console.error(`[Repository] Failed to read ride ${rideIds[index]}`, error);
      }
      return [];
    }

    try {
      return [JSON.parse(value) as Ride];
    } catch (parseError) {
      console.error(`[Repository] Invalid ride data for ${rideIds[index]}`, parseError);
      return [];
    }
  });
}

export async function updateRide(
  rideId: string,
  updater: (current: Ride) => Ride,
): Promise<Ride | null> {
  const currentRide = await getRideById(rideId);
  if (currentRide === null) {
    return null;
  }

  const updatedRide = {
    ...updater(currentRide),
    updatedAt: new Date().toISOString(),
  };
  await redis.set(rideKey(rideId), JSON.stringify(updatedRide));
  return updatedRide;
}

export async function acquireDriverLock(
  driverId: string,
  rideId: string,
  ttlMs = 3000,
): Promise<boolean> {
  const result = await redis.set(driverLockKey(driverId), rideId, "PX", ttlMs, "NX");
  return result === "OK";
}

export async function releaseDriverLock(driverId: string, rideId: string): Promise<void> {
  const releaseScript = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
      return redis.call("del", KEYS[1])
    end
    return 0
  `;

  await redis.eval(releaseScript, 1, driverLockKey(driverId), rideId);
}

export async function closeRepositoryRedis(): Promise<void> {
  await redis.quit();
}