// disk-cache-s3-store.js   v5 – single-part cache
import fs, { promises as fsProm } from 'node:fs'
import path from 'node:path'
import os from 'node:os'
import { pipeline } from 'node:stream/promises'
import { S3Store } from '@tus/s3-store'
import { Upload } from '@tus/utils'
import cron from "node-cron"

const TTL_MS = 0.001 * 24 * 60 * 60 * 1000      // 2 days
const CRON_EXPR = '0 */6 * * *'             // every 6 h

export class DiskCacheS3Store extends S3Store {
  constructor(opts) {
    super(opts)
    this.tmpDir = opts.tmpDir || path.join(os.tmpdir(), 'tus-disk-cache')
    fs.mkdirSync(this.tmpDir, { recursive: true })
    this._state = new Map()    // id → { flushing:Promise, tempSize:number }

    this._reaperJob = cron.schedule(
      CRON_EXPR,
      () => this.#sweepCache().catch(() => { }),
      { timezone: 'UTC' }
    )
    this.#sweepCache()
  }

  /* --------- tus API --------- */
  async write(src, id, offset) {
    const safeId = id.replaceAll('/', '_')
    const cachePath = path.join(this.tmpDir, `${safeId}.part`)
    const st = this._state.get(id) ?? { flushing: Promise.resolve(), tempSize: 0 }

    await st.flushing                                             // wait if a flush is running

    const before = await this.#fileSize(cachePath)
    await pipeline(src, fs.createWriteStream(cachePath, { flags: 'a' }))
    const after = await this.#fileSize(cachePath)
    const delta = after - before
    st.tempSize = after

    const meta = await this.getMetadata(id)
    const complete = meta.file.size !== undefined && offset + delta >= meta.file.size
    const needFlush = st.tempSize >= this.minPartSize || complete

    if (needFlush) {
      st.flushing = this.#flush(id, cachePath, st.tempSize, complete)
      await st.flushing
      st.tempSize = 0
    }

    this._state.set(id, st)
    return offset + delta
  }

  async getUpload(id) {
    const base = await super.getUpload(id)
    const safeId = id.replaceAll('/', '_')
    const pending = await this.#fileSize(path.join(this.tmpDir, `${safeId}.part`))
    return new Upload({ ...base, offset: base.offset + pending })
  }

  /* --------- internals --------- */
  async #flush(id, filePath, size, final) {
    const meta = await this.getMetadata(id)
    const parts = await this.retrieveParts(id)
    const num = parts.length + 1
    const stream = fs.createReadStream(filePath)

    if (size >= this.minPartSize || final) {
      await this.uploadPart(meta, stream, num)
    } else {
      await this.uploadIncompletePart(id, stream)
    }
    await fsProm.unlink(filePath)

    if (final) {
      const all = await this.retrieveParts(id)
      await this.finishMultipartUpload(meta, all)
      await this.completeMetadata(meta.file)
      this._state.delete(id)
    }
  }

  async #fileSize(p) {
    try { return (await fsProm.stat(p)).size } catch { return 0 }
  }

  async #sweepCache() {
    console.log('Sweeping disk cache:', this.tmpDir)
    const now = Date.now()

    for (const name of await fsProm.readdir(this.tmpDir)) {
      if (!name.endsWith('.part')) continue           // only cache files

      const id = name.slice(0, -5).replaceAll('_', '/')
      const file = path.join(this.tmpDir, name)
      const { mtimeMs } = await fsProm.stat(file)

      if (now - mtimeMs <= TTL_MS) continue          // keep if newer than 2 days

      console.log('Removing expired cache for id:', id, 'file:', file)

      try {
        // abort MPU, delete metadata + cache, unlink file, drop from _state. Also runs the clearCache function overriden in this class
        await this.remove(id)
      } catch (err) {
        // ignore “already removed” errors, log anything unexpected
        const code = err?.code ?? err?.Code
        if (!['FILE_NOT_FOUND', 'NotFound', 'NoSuchKey', 'NoSuchUpload'].includes(code)) {
          console.warn('sweepCache:', err)
        }
      }
    }
  }

  async clearCache(id) {
    // 1) run the parent logic (removes entry from the upstream LRU cache, etc.)
    await super.clearCache?.(id)     // optional chaining in case parent is not async

    // 2) delete the on-disk part file
    const safe = id.replaceAll('/', '_')
    const part = path.join(this.tmpDir, `${safe}.part`)
    await fsProm.unlink(part).catch(() => { })   // ignore ENOENT

    // 3) drop the in-memory bookkeeping so offsets stay correct
    this._state.delete(id)
  }
}
