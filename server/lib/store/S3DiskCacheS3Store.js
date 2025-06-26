// disk-cache-s3-store.js  (v4 – capped cache + wait-last)
import fs, { promises as fsProm } from 'node:fs'
import path from 'node:path'
import os from 'node:os'
import { pipeline } from 'node:stream/promises'
import { randomBytes } from 'crypto'
import { S3Store } from '@tus/s3-store'
import { Upload } from '@tus/utils'

export class DiskCacheS3Store extends S3Store {
  constructor(opts) {
    super(opts)
    this.tmpDir = opts.tmpDir || path.join(os.tmpdir(), 'tus-disk-cache')
    this.maxCachedChunks = opts.maxCachedChunks ?? 4       // ← cap
    fs.mkdirSync(this.tmpDir, { recursive: true })

    /** id → { chain:Promise, accepted:number, queued:number, waiters:Function[] } */
    this._queues = new Map()
  }

  /** 1 PATCH = save to temp → enqueue flush; pause if >maxCachedChunks */
  async write(src, id, offset) {
    const q = this._queues.get(id) ?? {
      chain: Promise.resolve(),
      accepted: offset,
      queued: 0,
      waiters: []
    }

    while (q.queued >= this.maxCachedChunks) {
      await new Promise(r => q.waiters.push(r))   // back-pressure
    }

    const safeId = id.replaceAll('/', '_')        // keep your slash-to-underscore rule
    const fname = path.join(
      this.tmpDir,
      `${safeId}-${offset}-${randomBytes(6).toString('base64url')}.part`
    )

    await pipeline(src, fs.createWriteStream(fname))
    const size = (await fsProm.stat(fname)).size

    q.queued++
    q.accepted += size

    /* enqueue background flush, keep order */
    q.chain = q.chain.then(() =>
      this.#flush(id, fname, offset, size, q)
    ).catch(() => { })          // swallow to keep chain alive
    this._queues.set(id, q)

    /* ---------- wait if this was the FINAL chunk ---------- */
    const meta = await this.getMetadata(id)       // read once per PATCH
    const done = meta.file.size !== undefined && q.accepted >= meta.file.size
    if (done) await q.chain                       // block until flush & MPU complete

    return q.accepted
  }

  /** make pending bytes visible to HEAD / next PATCH */
  async getUpload(id) {
    const base = await super.getUpload(id)
    const pending = this._queues.get(id)?.accepted ?? base.offset
    return new Upload({ ...base, offset: pending })
  }

  /* ---------- internal helpers ---------- */
  async #flush(id, filePath, offset, size, q) {
    const meta = await this.getMetadata(id)
    const parts = await this.retrieveParts(id)
    const num = parts.length + 1
    const done = meta.file.size === offset + size

    const stream = fs.createReadStream(filePath)
    if (size >= this.minPartSize || done) {
      await this.uploadPart(meta, stream, num)
    } else {
      await this.uploadIncompletePart(id, stream)
    }
    await fsProm.unlink(filePath).catch(() => { })

    /* unlock one waiter if cache below cap */
    q.queued--
    if (q.waiters.length && q.queued < this.maxCachedChunks) q.waiters.shift()()

    /* finalise upload */
    if (done) {
      const all = await this.retrieveParts(id)
      await this.finishMultipartUpload(meta, all)
      await this.completeMetadata(meta.file)
      await this.clearCache(id)
      this._queues.delete(id)
    }
  }
}
