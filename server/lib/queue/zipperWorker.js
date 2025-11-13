import IORedis from "ioredis"
import { provider } from "../provider/provider.js";
import { Worker } from "bullmq";
import { REDIS_URI } from "../redis.js";

const buildVerboseJobLog = job => {
  const filesList = job?.data?.filesList ?? [];
  const totalSize = filesList.reduce((sum, file) => sum + (file?.size ?? 0), 0);
  const duration = typeof job?.timestamp === "number"
    ? `${Date.now() - job.timestamp}ms`
    : undefined;

  return {
    ...(duration !== undefined ? { duration } : {}),
    totalSize: `${totalSize} bytes`,
    numberOfFiles: filesList.length,
  };
};

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
    {
      connection,
      concurrency: 3
    }
  );

  worker.on("failed", async (job, err) => {
    const verboseLog = buildVerboseJobLog(job);

    if (job.attemptsMade >= job.opts.attempts) {
      const logEntry = {
        id: job.id,
        name: job.name,
        data: job.data,
        reason: err.message,
        attempts: job.attemptsMade,
        failedAt: new Date().toISOString(),
        // ...verboseLog,
      };
      console.error(`[FAILED+DEAD JOB] ${job.id}:`, JSON.stringify(logEntry));
    }
    else {
      console.error(`[FAILED JOB] ${job.id}:`, err, verboseLog)
    }
  })

  worker.on("completed", async (job, result) => {
    const verboseLog = buildVerboseJobLog(job);
    console.log(`[COMPLETED JOB] ${job.id}:`, {
      result,
      ...verboseLog
    })
  })

  worker.on("active", async job => {
    console.log(`[ACTIVE JOB] ${job.id}`)
  })

  return worker
}

export default startWorker
