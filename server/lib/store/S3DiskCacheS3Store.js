// disk-cache-s3-store.js   v5 – single-part cache
import fs, { promises as fsProm } from 'node:fs'
import path from 'node:path'
import os from 'node:os'
import { pipeline } from 'node:stream/promises'
import { S3Store } from '@tus/s3-store'
import { Upload, TUS_RESUMABLE, ERRORS } from '@tus/utils'
import cron from "node-cron"

const DEBUG = true
const DEBUG_FAST_SWEEP = false

const TTL_MS = DEBUG_FAST_SWEEP ? 30 * 1000 : 10 * 60 * 1000       // 30 seconds if FAST_SWEEP, else 10min
const CRON_EXPR = DEBUG_FAST_SWEEP ? '*/15 * * * * *' : '*/5 * * * *'       // every 15 sec if FAST_SWEEP, else every 5 min

export class DiskCacheS3Store extends S3Store {
  constructor(opts) {
    super(opts)
    this.tmpDir = opts.tmpDir || path.join(os.tmpdir(), 'tus-disk-cache')
    fs.mkdirSync(this.tmpDir, { recursive: true })
    this._state = new Map()    // id → { flushing:Promise, tempSize:number }

    this._reaperJob = cron.schedule(
      CRON_EXPR,
      () => this.#sweepCache(false).catch(() => { }),
      { timezone: 'UTC' }
    )
  }

  async init() {
    await this.#sweepCache(true)
  }/**
   * Override S3Store.create() so that 0-byte uploads are finished
   * immediately instead of opening an MPU that will never receive parts.
   */
  async create(upload) {
    // ── 1. If the upload is not empty, fall back to the parent logic ─────────
    if (upload.size !== 0 && upload.upload_length !== 0) {
      return super.create(upload)
    }

    // ── 2. Allocate the object key and metadata --------------------------------
    const id = upload.id
    const key = id                           // same as S3Store
    const infoKey = this.infoKey(id)

    // (a) Put an empty object in S3
    await this.client.putObject({
      Bucket: this.bucket,
      Key: key,
      Body: '',                            // zero-byte body
      Metadata: { 'tus-version': TUS_RESUMABLE }
    })

    // (b) Build a minimal .info object (NO "upload-id")
    const info = {
      file: {
        id,
        size: 0,
        metadata: upload.metadata ?? {},
        creation_date: new Date().toISOString()
      },
      // no upload-id  →  MPU already “complete”
    }

    await this.client.putObject({
      Bucket: this.bucket,
      Key: infoKey,
      Body: JSON.stringify(info)
    })

    // ── 3. Return the same shape that S3Store.create() would -------------------
    upload.storage = {
      type: 's3',
      path: key,
      bucket: this.bucket
    }
    upload.creation_date = info.file.creation_date
    upload.upload_length = 0                 // guarantee the property is set

    if (DEBUG) {
      console.log(`[${id}] zero-byte upload finalised immediately`)
    }
    return upload
  }

  /* --------- tus API --------- */
  async write(src, id, offset) {
    const safeId = id.replaceAll('/', '_')
    const cachePath = path.join(this.tmpDir, `${safeId}.part`)
    const st = this._state.get(id) ?? { flushing: Promise.resolve(), tempSize: 0 }

    await st.flushing                                             // wait if a flush is running

    // append the new chunk to the on-disk cache file
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

    if (DEBUG) {
      console.log(
        '[DiskCacheS3Store:getUpload]',
        { id, baseOffset: base.offset, pending }
      )
    }

    return new Upload({ ...base, offset: base.offset + pending })
  }

  /* --------- internals --------- */
  async #flush(id, filePath, size, final) {
    const meta = await this.getMetadata(id)
    const parts = await this.retrieveParts(id)
    const num = parts.length + 1
    const stream = fs.createReadStream(filePath)

    if (DEBUG) {
      console.log('[DiskCacheS3Store:#flush]', { id, size, minPartSize: this.minPartSize, final })
    }

    if (size >= this.minPartSize || final) {
      if (DEBUG) {
        console.log('[DiskCacheS3Store:#flush] uploading part', { id, num })
      }
      await this.uploadPart(meta, stream, num)
    } else {
      if (DEBUG) {
        console.log('[DiskCacheS3Store:#flush] uploading incomplete part', { id })
      }
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

  async #sweepCache(all) {
    if (all) {
      console.log('Sweeping ALL disk cache:', this.tmpDir)
    }
    else {
      console.log('Sweeping disk cache:', this.tmpDir)
    }
    const now = Date.now()

    for (const name of await fsProm.readdir(this.tmpDir)) {
      if (!name.endsWith('.part')) continue           // only cache files

      const id = name.slice(0, -5).replaceAll('_', '/')
      const file = path.join(this.tmpDir, name)
      const { mtimeMs } = await fsProm.stat(file)

      if (all) {
        // remove all, usually ran after a reboot to not desync the _state and Disk cache
        // which would lead to a lot of problems. We also do NOT want to delete multipart
        // uploads on reboot, in case an upload is in process while node reboots
        console.log('Removing previous leftover cache for id:', id, 'file:', file)

        // delete the on-disk part file
        await fsProm.unlink(file).catch(() => { })
      }
      else {
        if (now - mtimeMs <= TTL_MS) continue          // keep if newer than 2 days

        console.log('Removing expired cache for id:', id, 'file:', file)

        try {
          // This aborts the multipart upload and deletes the info key from the bucket
          this.remove(id)
        }
        catch (err) {
          // this.remove(id) throws either if:
          // 1. this.getMetadata(id) doesnt exist
          // 2. and maybe? this.client.abortMultipartUpload throws
        }
        finally {
          // 1) run the parent logic (removes entry from the upstream LRU cache, etc.)
          await super.clearCache?.(id)     // optional chaining in case parent is not async

          // 2) delete the on-disk part file
          await fsProm.unlink(file).catch(() => { })

          // 3) drop the in-memory bookkeeping so offsets stay correct
          this._state.delete(id)
        }
      }
    }

    console.log("Done sweeping cache!")
  }
}
