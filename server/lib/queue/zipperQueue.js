import { Queue } from "bullmq"
import IORedis from "ioredis"
import { REDIS_URI } from "../redis.js";

const connection = new IORedis(REDIS_URI, { maxRetriesPerRequest: null });

const zipperQueue = new Queue("zipper", {
  connection,
});

export default zipperQueue