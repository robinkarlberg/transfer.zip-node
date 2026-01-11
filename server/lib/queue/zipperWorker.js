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
    {
      connection,
      concurrency: 3
    }
  );


  return worker
}

export default startWorker
