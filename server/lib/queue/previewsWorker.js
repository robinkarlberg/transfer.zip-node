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
const startPreviewsWorker = (logger) => {
  const worker = new Worker(
    "previews",
    async job => {
      const { filesList } = job.data
      const transferId = job.id

      const result = await provider.createPreviews(transferId, filesList, logger.child({ action: "previews", transferId }))
      return result
    },
    {
      connection,
      // keep image decoding from starving the zipper workers
      concurrency: 2
    }
  )

  return worker
}

export default startPreviewsWorker
