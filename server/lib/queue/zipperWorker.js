import IORedis from "ioredis"
import { provider } from "../provider/provider.js";
import { Worker } from "bullmq";
import { REDIS_URI } from "../redis.js";
import pino from "pino";

const connection = new IORedis(REDIS_URI, { maxRetriesPerRequest: null });

/**
 * 
 * @param {pino.Logger} logger 
 * @returns {Worker} the worker
 */
const startWorker = (logger) => {
  const worker = new Worker(
    "zipper",
    async job => {
      const { filesList } = job.data
      const transferId = job.id

      const result = await provider.createZipBundle(transferId, filesList, logger.child({ action: "zipper", transferId }))
      return result
    },
    {
      connection,
      concurrency: 3
    }
  )

  return worker
}

export default startWorker
