import { Queue } from "bullmq"
import IORedis from "ioredis"
import { REDIS_URI } from "../redis.js";

const connection = new IORedis(REDIS_URI, { maxRetriesPerRequest: null });

const previewsQueue = new Queue("previews", {
  connection,
});

export default previewsQueue
