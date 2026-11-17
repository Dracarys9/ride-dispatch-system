import "dotenv/config";
import { Job, Worker } from "bullmq";
import { createRedisClient, DISPATCH_QUEUE_NAME } from "../config/redis";
import { DRIVERS } from "../data/drivers";
import {
  acquireDriverLock,
  closeRepositoryRedis,
  getRideById,
  releaseDriverLock,
  updateRide,
} from "../repositories/rideRepository";
import { closePublisherRedis, publishRideEvent } from "../events/eventPublisher";
import { DispatchAttempt, Driver } from "../types/ride";

const MAX_REJECTIONS = 3;

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function pickAvailableDriver(triedDriverIds: Set<string>): Driver | null {
  const availableDrivers = DRIVERS.filter((driver) => !triedDriverIds.has(driver.id));
  if (availableDrivers.length === 0) {
    return null;
  }

  const randomIndex = Math.floor(Math.random() * availableDrivers.length);
  return availableDrivers[randomIndex];
}

export async function simulateDriverResponse(forceReject = false): Promise<boolean> {
  const latency = 150 + Math.floor(Math.random() * 201);
  await sleep(latency);
  if (forceReject) {
    return false;
  }
  return Math.random() < 0.5;
}

export async function processRideDispatch(
  job: Job<{ rideId: string; forceReject?: boolean }>,
): Promise<void> {
  const { rideId } = job.data;
  const ride = await getRideById(rideId);

  if (!ride || ride.status !== "REQUESTED") {
    console.log(`[WORKER] Skipping ride ${rideId}: ride is missing or already processed`);
    return;
  }

  const forceReject = Boolean(job.data.forceReject || ride.forceReject);
  const triedDriverIds = new Set<string>();
  let rejections = 0;

  while (rejections < MAX_REJECTIONS) {
    const driver = pickAvailableDriver(triedDriverIds);
    if (driver === null) {
      break;
    }

    const lockAcquired = await acquireDriverLock(driver.id, rideId, 3000);
    if (!lockAcquired) {
      await sleep(50);
      continue;
    }

    try {
      triedDriverIds.add(driver.id);
      const accepted = await simulateDriverResponse(forceReject);
      const attempt: DispatchAttempt = {
        attemptNumber: triedDriverIds.size,
        driverId: driver.id,
        driverName: driver.name,
        accepted,
        timestamp: new Date().toISOString(),
      };

      if (accepted) {
        await updateRide(rideId, (current) => ({
          ...current,
          status: "ASSIGNED",
          assignedDriverId: driver.id,
          attempts: [...current.attempts, attempt],
        }));
        await publishRideEvent({
          rideId,
          status: "ASSIGNED",
          driverId: driver.id,
        });
        console.log(
          `[WORKER] Ride ${rideId} ASSIGNED to ${driver.name} (${driver.id}) on attempt ${attempt.attemptNumber}`,
        );
        return;
      }

      rejections++;
      await updateRide(rideId, (current) => ({
        ...current,
        attempts: [...current.attempts, attempt],
      }));
      console.log(
        `[WORKER] Ride ${rideId} rejected by ${driver.name} (${rejections}/${MAX_REJECTIONS} rejections)`,
      );
    } finally {
      await releaseDriverLock(driver.id, rideId);
    }
  }

  await updateRide(rideId, (current) => ({
    ...current,
    status: "NO_DRIVER_FOUND",
    assignedDriverId: null,
  }));
  await publishRideEvent({
    rideId,
    status: "NO_DRIVER_FOUND",
    driverId: null,
  });
  console.log(
    `[WORKER] Ride ${rideId} marked NO_DRIVER_FOUND after ${rejections} rejections`,
  );
}

const concurrency = Number(process.env.WORKER_CONCURRENCY) || 10;
const worker = new Worker(DISPATCH_QUEUE_NAME, processRideDispatch, {
  connection: createRedisClient("worker"),
  concurrency,
});

worker.on("failed", (job, error) => {
  console.error(`[WORKER] Job ${job?.id ?? "unknown"} failed`, error);
});

let isShuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;
  console.log(`[WORKER] Received ${signal}, shutting down`);

  await worker.close();
  await closePublisherRedis();
  await closeRepositoryRedis();
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void shutdown(signal)
      .then(() => process.exit(0))
      .catch((error) => {
        console.error("[WORKER] Shutdown failed", error);
        process.exit(1);
      });
  });
}