import IORedis from "ioredis"
import { provider } from "../provider/provider.js";
import { Worker } from "bullmq";
import { REDIS_URI } from "../redis.js";

const connection = new IORedis(REDIS_URI, { maxRetriesPerRequest: null });

const startWorker = () => {
  const worker = new Worker(
    "zipper",
    async job => {
      console.log(`Processing job ${job.id}`)
      try {
        const { filesList } = job.data
        const transferId = job.id
        
        const result = await provider.createZipBundle(transferId, filesList)
      }
      catch (err) {
        console.error(err)
        throw err
      }
      console.log(`Job ${job.id} finished:`, result)
    },
    { connection }
  );
  return worker
}

export default startWorker