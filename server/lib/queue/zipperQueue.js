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
  jobsSorted.map(async job => {
    const state = await job.getState()
    return {
      job,
      state
    }
  })
)

const activeJobsWithState = jobsWithState
  .filter(({ state }) => state !== 'completed')
  .map(({ job, state }) => [
    job.name,
    job.failedReason,
    state,
    new Date(job.timestamp).toISOString()
  ])

console.log(
  "\n" +
  [
    ["Name", "Reason", "State", "Timestamp"],
    ["=====", "=====", "=====", "========="],
    ...activeJobsWithState
  ]
    .map(row => row.map(cell => String(cell).padEnd(42)).join("| "))
    .join("\n")
)

export default zipperQueue