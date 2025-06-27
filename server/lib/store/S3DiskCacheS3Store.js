// disk-cache-s3-store.js   v5 – single-part cache
import fs, { promises as fsProm } from 'node:fs'
import path from 'node:path'
import os from 'node:os'
import { pipeline } from 'node:stream/promises'
import { S3Store } from '@tus/s3-store'
import { Upload } from '@tus/utils'

export class DiskCacheS3Store extends S3Store {
  constructor (opts) {
    super(opts)
    this.tmpDir = opts.tmpDir || path.join(os.tmpdir(), 'tus-disk-cache')
    fs.mkdirSync(this.tmpDir, { recursive: true })
    this._state = new Map()    // id → { flushing:Promise, tempSize:number }
  }

  /* --------- tus API --------- */
  async write (src, id, offset) {
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

  async getUpload (id) {
    const base = await super.getUpload(id)
    const safeId = id.replaceAll('/', '_')
    const pending = await this.#fileSize(path.join(this.tmpDir, `${safeId}.part`))
    return new Upload({ ...base, offset: base.offset + pending })
  }

  /* --------- internals --------- */
  async #flush (id, filePath, size, final) {
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

  async #fileSize (p) {
    try { return (await fsProm.stat(p)).size } catch { return 0 }
  }
}
