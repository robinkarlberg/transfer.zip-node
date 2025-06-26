import IORedis from "ioredis"
import { provider } from "../provider/provider.js";
import { Worker } from "bullmq";
import { REDIS_URI } from "../redis.js";

const connection = new IORedis(REDIS_URI, { maxRetriesPerRequest: null });

const startWorker = () => {
  const worker = new Worker(
    "zipper",
    async job => {
      const { filesList } = job.data
      const transferId = job.id

      const result = await provider.createZipBundle(transferId, filesList)
      return result
    },
    { connection }
  );

  worker.on("failed", async (job, err) => {
    if (job.attemptsMade >= job.opts.attempts) {
      const logEntry = {
        id: job.id,
        name: job.name,
        data: job.data,
        reason: err.message,
        attempts: job.attemptsMade,
        failedAt: new Date().toISOString(),
      };
      console.error(`[FAILED+DEAD JOB] ${job.id}:`, JSON.stringify(logEntry));
    }
    else {
      console.error(`[FAILED JOB] ${job.id}:`, err)
    }
  })

  worker.on("completed", async (job, result) => {
    console.log(`[COMPLETED JOB] ${job.id}:`, result)
  })

  worker.on("active", async job => {
    console.log(`[ACTIVE JOB] ${job.id}`)
  })

  return worker
}

export default startWorker