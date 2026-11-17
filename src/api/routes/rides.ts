import { Queue } from "bullmq";
import { Request, Response, Router } from "express";
import { v4 as uuidv4 } from "uuid";
import { z } from "zod";
import {
  createRedisClient,
  DISPATCH_QUEUE_NAME,
  RIDE_EVENTS_CHANNEL,
} from "../../config/redis";
import { publishRideEvent } from "../../events/eventPublisher";
import {
  claimIdempotencyKey,
  createRide,
  getRideById,
  releaseIdempotencyKey,
} from "../../repositories/rideRepository";
import { Ride, RideEvent } from "../../types/ride";

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
  const forceReject =
    req.query.forceReject === "true" ||
    req.header("X-Simulate-Reject")?.toLowerCase() === "true";
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
      forceReject: Boolean(forceReject),
    };

    await createRide(ride);
    ridePersisted = true;
    await dispatchQueue.add(
      "dispatch-ride",
      { rideId: candidateRideId, forceReject },
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

router.get("/:rideId/stream", async (req: Request, res: Response, next) => {
  const { rideId } = req.params;
  if (typeof rideId !== "string") {
    res.status(400).json({ error: "Invalid ride ID" });
    return;
  }

  try {
    const existingRide = await getRideById(rideId);
    if (existingRide === null) {
      res.status(404).json({ error: "Ride not found" });
      return;
    }

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();

    const sendEvent = (eventName: string, payload: unknown): void => {
      if (!res.writableEnded) {
        res.write(
          `event: ${eventName}\ndata: ${JSON.stringify(payload)}\n\n`,
        );
      }
    };

    sendEvent("status", {
      rideId: existingRide.rideId,
      status: existingRide.status,
      assignedDriverId: existingRide.assignedDriverId,
      timestamp: existingRide.updatedAt,
    });

    if (
      existingRide.status === "ASSIGNED" ||
      existingRide.status === "NO_DRIVER_FOUND"
    ) {
      res.end();
      return;
    }

    const subClient = createRedisClient("sse");
    let cleanedUp = false;

    const cleanup = async (): Promise<void> => {
      if (cleanedUp) {
        return;
      }
      cleanedUp = true;
      await subClient.unsubscribe(RIDE_EVENTS_CHANNEL);
      await subClient.quit();
    };

    req.on("close", () => {
      void cleanup().catch((error) => {
        console.error(`[API] Failed to clean up SSE stream for ${rideId}`, error);
      });
    });

    subClient.on("message", (_channel, message) => {
      let event: RideEvent;
      try {
        event = JSON.parse(message) as RideEvent;
      } catch (error) {
        console.error("[API] Failed to parse ride event for SSE stream", error);
        return;
      }

      if (event.rideId !== existingRide.rideId || cleanedUp) {
        return;
      }

      sendEvent("status", event);
      if (
        event.status === "ASSIGNED" ||
        event.status === "NO_DRIVER_FOUND"
      ) {
        void cleanup()
          .then(() => {
            if (!res.writableEnded) {
              res.end();
            }
          })
          .catch((error) => {
            console.error(
              `[API] Failed to close SSE stream for ${rideId}`,
              error,
            );
            if (!res.writableEnded) {
              res.end();
            }
          });
      }
    });

    try {
      await subClient.subscribe(RIDE_EVENTS_CHANNEL);
    } catch (error) {
      console.error(
        `[API] Failed to subscribe SSE stream for ${rideId}`,
        error,
      );
      await cleanup();
      if (!res.writableEnded) {
        res.end();
      }
    }
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