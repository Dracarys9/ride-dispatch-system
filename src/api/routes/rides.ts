import { Queue } from "bullmq";
import { Request, Response, Router } from "express";
import { v4 as uuidv4 } from "uuid";
import { z } from "zod";
import { createRedisClient, DISPATCH_QUEUE_NAME } from "../../config/redis";
import { publishRideEvent } from "../../events/eventPublisher";
import {
  claimIdempotencyKey,
  createRide,
  getRideById,
  releaseIdempotencyKey,
} from "../../repositories/rideRepository";
import { Ride } from "../../types/ride";

const queueRedis = createRedisClient("queue");
const dispatchQueue = new Queue(DISPATCH_QUEUE_NAME, {
  connection: queueRedis,
});

export const createRideSchema = z.object({
  riderId: z.string().default("rider_guest"),
  pickupLocation: z.string().default("Bandra West, Mumbai"),
  dropoffLocation: z.string().default("Andheri East, Mumbai"),
});

export const router = Router();

router.post("/", async (req: Request, res: Response, next) => {
  const idempotencyKey = req.header("Idempotency-Key")?.trim();
  let idempotencyClaimed = false;
  let ridePersisted = false;

  try {
    const parsedInput = createRideSchema.safeParse(req.body);
    if (!parsedInput.success) {
      res.status(400).json({
        error: "Invalid ride input",
        details: parsedInput.error.issues,
      });
      return;
    }

    const candidateRideId = `ride_${uuidv4().slice(0, 8)}`;
    if (idempotencyKey) {
      const resolvedRideId = await claimIdempotencyKey(
        idempotencyKey,
        candidateRideId,
      );
      if (resolvedRideId !== candidateRideId) {
        const existingRide = await getRideById(resolvedRideId);
        res.status(200).json({
          rideId: resolvedRideId,
          status: existingRide?.status ?? "REQUESTED",
          idempotentReplay: true,
          message: "Existing ride returned for Idempotency-Key",
        });
        return;
      }
      idempotencyClaimed = true;
    }

    const timestamp = new Date().toISOString();
    const ride: Ride = {
      rideId: candidateRideId,
      riderId: parsedInput.data.riderId,
      pickupLocation: parsedInput.data.pickupLocation,
      dropoffLocation: parsedInput.data.dropoffLocation,
      status: "REQUESTED",
      assignedDriverId: null,
      attempts: [],
      createdAt: timestamp,
      updatedAt: timestamp,
    };

    await createRide(ride);
    ridePersisted = true;
    await dispatchQueue.add(
      "dispatch-ride",
      { rideId: candidateRideId },
      { removeOnComplete: 500, removeOnFail: 500 },
    );
    await publishRideEvent({
      rideId: candidateRideId,
      status: ride.status,
      driverId: ride.assignedDriverId,
    });

    res.status(202).json({
      rideId: candidateRideId,
      status: ride.status,
      message: "Ride booked and queued for dispatch",
    });
  } catch (error) {
    if (idempotencyKey && idempotencyClaimed && !ridePersisted) {
      try {
        await releaseIdempotencyKey(idempotencyKey);
      } catch (releaseError) {
        console.error(
          `[API] Failed to release Idempotency-Key ${idempotencyKey}`,
          releaseError,
        );
      }
    }
    next(error);
  }
});

router.get("/:rideId", async (req: Request, res: Response, next) => {
  try {
    const { rideId } = req.params;
    if (typeof rideId !== "string") {
      res.status(400).json({ error: "Invalid ride ID" });
      return;
    }

    const ride = await getRideById(rideId);
    if (ride === null) {
      res.status(404).json({ error: "Ride not found" });
      return;
    }

    res.status(200).json(ride);
  } catch (error) {
    next(error);
  }
});

export async function closeDispatchQueue(): Promise<void> {
  await dispatchQueue.close();
}

export default router;