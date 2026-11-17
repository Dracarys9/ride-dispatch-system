import { Queue } from "bullmq";
import { Request, Response, Router } from "express";
import { v4 as uuidv4 } from "uuid";
import { z } from "zod";
import { createRedisClient, DISPATCH_QUEUE_NAME } from "../../config/redis";
import { publishRideEvent } from "../../events/eventPublisher";
import {
  createRide,
  getRideById,
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
  try {
    const parsedInput = createRideSchema.safeParse(req.body);
    if (!parsedInput.success) {
      res.status(400).json({
        error: "Invalid ride input",
        details: parsedInput.error.issues,
      });
      return;
    }

    const rideId = `ride_${uuidv4().slice(0, 8)}`;
    const timestamp = new Date().toISOString();
    const ride: Ride = {
      rideId,
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
    await dispatchQueue.add(
      "dispatch-ride",
      { rideId },
      { removeOnComplete: 500, removeOnFail: 500 },
    );
    await publishRideEvent({
      rideId,
      status: ride.status,
      driverId: ride.assignedDriverId,
    });

    res.status(202).json({
      rideId,
      status: ride.status,
      message: "Ride booked and queued for dispatch",
    });
  } catch (error) {
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