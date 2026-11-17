import "dotenv/config";
import Table from "cli-table3";
import { Ride } from "../types/ride";

const API_URL = process.env.API_URL || "http://localhost:3000";
const TOTAL_RIDES = 100;
const POLL_INTERVAL_MS = 250;
const POLL_BATCH_SIZE = 20;

interface RideBooking {
  rideId: string;
  statusCode: number;
}

interface CheckResult {
  check: string;
  expected: string;
  actual: string;
  result: "PASS" | "FAIL";
}

export async function bookRide(index: number): Promise<RideBooking> {
  const response = await fetch(`${API_URL}/rides`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      riderId: `rider_${String(index + 1).padStart(3, "0")}`,
      pickupLocation: "Bandra West, Mumbai",
      dropoffLocation: "Powai, Mumbai",
    }),
  });
  const body = (await response.json()) as { rideId?: unknown };

  if (typeof body.rideId !== "string" || body.rideId.length === 0) {
    throw new Error(`POST /rides returned an invalid rideId at index ${index}`);
  }

  return { rideId: body.rideId, statusCode: response.status };
}

export async function fetchRide(rideId: string): Promise<Ride> {
  const response = await fetch(`${API_URL}/rides/${encodeURIComponent(rideId)}`);
  if (!response.ok) {
    throw new Error(`GET /rides/${rideId} returned HTTP ${response.status}`);
  }
  return (await response.json()) as Ride;
}

async function fetchRideBatch(rideIds: string[]): Promise<Ride[]> {
  const rides: Ride[] = [];
  for (let index = 0; index < rideIds.length; index += POLL_BATCH_SIZE) {
    const batch = rideIds.slice(index, index + POLL_BATCH_SIZE);
    rides.push(...(await Promise.all(batch.map(fetchRide))));
  }
  return rides;
}

export async function waitForAllTerminalStates(
  rideIds: string[],
  timeoutMs = 30000,
): Promise<Ride[]> {
  const startedAt = Date.now();

  while (Date.now() - startedAt <= timeoutMs) {
    const rides = await fetchRideBatch(rideIds);
    const allTerminal = rides.every(
      (ride) => ride.status === "ASSIGNED" || ride.status === "NO_DRIVER_FOUND",
    );
    if (allTerminal) {
      return rides;
    }

    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }

  throw new Error(
    `Timed out after ${timeoutMs}ms waiting for ${rideIds.length} rides to reach terminal states`,
  );
}

export function calculatePercentile(
  sortedValues: number[],
  percentile: number,
): number {
  if (sortedValues.length === 0) {
    return 0;
  }

  const normalizedPercentile = percentile > 1 ? percentile / 100 : percentile;
  const position = (sortedValues.length - 1) * normalizedPercentile;
  const lowerIndex = Math.floor(position);
  const upperIndex = Math.ceil(position);
  const interpolation = position - lowerIndex;

  return (
    sortedValues[lowerIndex] +
    (sortedValues[upperIndex] - sortedValues[lowerIndex]) * interpolation
  );
}

function getDispatchLatency(ride: Ride): number {
  const latency =
    new Date(ride.updatedAt).getTime() - new Date(ride.createdAt).getTime();
  if (!Number.isFinite(latency)) {
    throw new Error(`Ride ${ride.rideId} contains invalid timestamps`);
  }
  return latency;
}

function printResults(
  checks: CheckResult[],
  batchDurationMs: number,
  averageLatencyMs: number,
  p95LatencyMs: number,
  driverDistribution: Map<string, number>,
): void {
  const invariantTable = new Table({
    head: ["Check", "Expected", "Actual", "Result"],
    colWidths: [28, 24, 28, 10],
  });
  checks.forEach((check) => {
    invariantTable.push([
      check.check,
      check.expected,
      check.actual,
      check.result,
    ]);
  });

  const performanceTable = new Table({
    head: ["Performance & Driver Utilization Summary", "Value"],
    colWidths: [36, 64],
  });
  const topDrivers = [...driverDistribution.entries()]
    .sort(([, countA], [, countB]) => countB - countA)
    .map(([driverId, count]) => `${driverId}: ${count}`)
    .join(" | ");
  performanceTable.push(
    ["Total Batch Wall Time", `${batchDurationMs} ms`],
    ["Avg Dispatch Latency", `${averageLatencyMs.toFixed(2)} ms`],
    ["P95 Dispatch Latency", `${p95LatencyMs.toFixed(2)} ms`],
    ["Top Assigned Drivers", topDrivers || "None"],
  );

  console.log("\nTask 3 Invariant Verification");
  console.log(invariantTable.toString());
  console.log("\nPerformance & Driver Utilization Summary");
  console.log(performanceTable.toString());
}

export async function runLoadTest(): Promise<void> {
  try {
    const healthResponse = await fetch(`${API_URL}/health`);
    if (!healthResponse.ok) {
      throw new Error(`health endpoint returned HTTP ${healthResponse.status}`);
    }
  } catch (error) {
    console.error(
      `[LOAD TEST] Cannot reach ${API_URL}. Start the stack with: docker compose up --build -d`,
      error,
    );
    throw error;
  }

  const batchStart = Date.now();
  const bookings = await Promise.all(
    Array.from({ length: TOTAL_RIDES }, (_, index) => bookRide(index)),
  );
  const invalidBookings = bookings.filter(
    (booking) => booking.statusCode !== 202,
  );
  const rideIds = bookings.map((booking) => booking.rideId);

  if (bookings.length !== TOTAL_RIDES || invalidBookings.length > 0) {
    throw new Error(
      `Expected ${TOTAL_RIDES} HTTP 202 responses, received ${bookings.length} bookings with ${invalidBookings.length} non-202 responses`,
    );
  }

  const finalRides = await waitForAllTerminalStates(rideIds);
  const batchDurationMs = Date.now() - batchStart;
  const assignedCount = finalRides.filter(
    (ride) => ride.status === "ASSIGNED",
  ).length;
  const noDriverCount = finalRides.filter(
    (ride) => ride.status === "NO_DRIVER_FOUND",
  ).length;
  const terminalTotal = assignedCount + noDriverCount;
  const doubleAssignedCount = finalRides.filter((ride) => {
    const acceptedAttempts = ride.attempts.filter((attempt) => attempt.accepted);
    return (
      acceptedAttempts.length > 1 ||
      (ride.status === "ASSIGNED" && !ride.assignedDriverId) ||
      (ride.status !== "ASSIGNED" && ride.assignedDriverId !== null)
    );
  }).length;
  const stuckRidesCount = finalRides.filter(
    (ride) => ride.status === "REQUESTED",
  ).length;
  const latencies = finalRides
    .map(getDispatchLatency)
    .sort((first, second) => first - second);
  const averageLatencyMs =
    latencies.reduce((total, latency) => total + latency, 0) /
    (latencies.length || 1);
  const p95LatencyMs = calculatePercentile(latencies, 0.95);
  const driverDistribution = new Map<string, number>();
  finalRides.forEach((ride) => {
    if (ride.assignedDriverId) {
      driverDistribution.set(
        ride.assignedDriverId,
        (driverDistribution.get(ride.assignedDriverId) || 0) + 1,
      );
    }
  });

  const checks: CheckResult[] = [
    {
      check: "Total rides created",
      expected: String(TOTAL_RIDES),
      actual: String(finalRides.length),
      result: finalRides.length === TOTAL_RIDES ? "PASS" : "FAIL",
    },
    {
      check: "Terminal rides total",
      expected: String(TOTAL_RIDES),
      actual: String(terminalTotal),
      result: terminalTotal === TOTAL_RIDES ? "PASS" : "FAIL",
    },
    {
      check: "Double-assigned/inconsistent rides",
      expected: "0",
      actual: String(doubleAssignedCount),
      result: doubleAssignedCount === 0 ? "PASS" : "FAIL",
    },
    {
      check: "Stuck REQUESTED rides",
      expected: "0",
      actual: String(stuckRidesCount),
      result: stuckRidesCount === 0 ? "PASS" : "FAIL",
    },
    {
      check: "ASSIGNED vs NO_DRIVER_FOUND",
      expected: `ASSIGNED + NO_DRIVER_FOUND = ${TOTAL_RIDES}`,
      actual: `${assignedCount} + ${noDriverCount} = ${terminalTotal}`,
      result: terminalTotal === TOTAL_RIDES ? "PASS" : "FAIL",
    },
  ];

  printResults(
    checks,
    batchDurationMs,
    averageLatencyMs,
    p95LatencyMs,
    driverDistribution,
  );

  if (checks.some((check) => check.result === "FAIL")) {
    throw new Error("One or more Task 3 invariants failed");
  }
}

void runLoadTest()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    console.error("[LOAD TEST] Failed", error);
    process.exit(1);
  });