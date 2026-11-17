import "dotenv/config";
import express, { ErrorRequestHandler } from "express";
import { createServer } from "node:http";
import { closePublisherRedis } from "../events/eventPublisher";
import { closeDispatchQueue, default as ridesRouter } from "./routes/rides";
import { closeRepositoryRedis } from "../repositories/rideRepository";

const app = express();
app.use(express.json());

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    service: "ride-dispatch-api",
    timestamp: new Date().toISOString(),
  });
});

app.use("/rides", ridesRouter);

const errorHandler: ErrorRequestHandler = (error, _req, res, _next) => {
  console.error("[API] Unhandled error", error);
  res.status(500).json({ error: "Internal server error" });
};
app.use(errorHandler);

const port = Number(process.env.PORT) || 3000;
const server = createServer(app);

const shutdown = async (signal: string): Promise<void> => {
  console.log(`[API] Received ${signal}, shutting down`);

  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
  await closeDispatchQueue();
  await closePublisherRedis();
  await closeRepositoryRedis();
};

server.listen(port, () => {
  console.log(`[API] Listening on port ${port}`);
});

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void shutdown(signal)
      .then(() => process.exit(0))
      .catch((error) => {
        console.error("[API] Shutdown failed", error);
        process.exit(1);
      });
  });
}