import { Queue } from "bullmq"
import IORedis from "ioredis"
import { REDIS_URI } from "../redis.js";

const connection = new IORedis(REDIS_URI, { maxRetriesPerRequest: null });

const zipperQueue = new Queue("zipper", {
  connection,
});

const jobs = await zipperQueue.getJobs()

const jobsSorted = [...jobs].sort((a, b) => b.timestamp - a.timestamp)

const jobsWithState = await Promise.all(
  jobsSorted.map(async job => [
    job.name,
    job.failedReason,
    await job.getState()
  ])
)

console.log(
  "\n" +
  [
    ["Name", "Reason", "State"],
    ["=====", "=====", "====="],
    ...jobsWithState
  ]
    .map(row => row.map(cell => String(cell).padEnd(50)).join("| "))
    .join("\n")
)

export default zipperQueue