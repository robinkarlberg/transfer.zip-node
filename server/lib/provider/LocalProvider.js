import archiver from "archiver"
import { deleteKeyRecurse, getObject, headBucket, listAllObjects, listBuckets, setAbortMultipartLifecycle, signDownload } from "../s3.js";
import { BaseProvider } from "./BaseProvider.js";
import { S3Client } from "@aws-sdk/client-s3";
import { finished, pipeline } from "stream/promises";
import fs from 'fs/promises'
import _fs from 'fs'
import path from 'path'
import { FileStore } from "@tus/file-store";

export class LocalProvider extends BaseProvider {
  constructor(config) {
    super(config)
    this.client = new S3Client(this.config.s3)

    this.datastore = new FileStore({
      directory: "/data/"
    })
    // this.client.config.credentials().then(console.log)
    // listBuckets(this.client).then(console.log)
    // listAllObjects(this.client, "kb-dev-0", "/").then(console.log)
    // console.log(this.config.s3)
    // console.log(this.client)
  }

  async init() {

  }

  getRootKey() {
    return process.env.NODE_ENV == "development" ? `../_local_dev_local_data/` : `/`
  }

  async hasBundle(transferId) {
    try {
      const path = this.getBundleKey(transferId)
      console.log(path)
      const stat = await fs.stat(path);
      return stat.isFile()
    } catch (err) {
      if (err.code === 'ENOENT') return false
      throw err;
    }
  }

  async listFiles(transferId) {
    super.listFiles()

    const dirPath = await this.getTransferFilesBaseKey(transferId)

    const files = await fs.readdir(dirPath, { withFileTypes: true })

    return Promise.all(
      files
        .filter(file => file.isFile())
        .map(async file => {
          const filePath = path.join(dirPath, file.name)
          const stat = await fs.stat(filePath)
          return {
            id: file.name,
            size: stat.size
          }
        })
    )
  }

  async createZipBundle(transferId, filesList) {
    const bundlePath = this.getBundleKey(transferId)
    await fs.mkdir(path.dirname(bundlePath), { recursive: true })

    const fileStream = await fs.open(bundlePath, 'w')

    const writeStream = fileStream.createWriteStream()

    await this.prepareZipBundleArchive(transferId, filesList, writeStream)

    await new Promise((resolve, reject) => {
      writeStream.on('close', resolve)
      writeStream.on('error', reject)
    })

    await fileStream.close()
    return { ok: true }
  }

  async prepareBundleSaved(transferId, fileName) {
    const path = this.getBundleKey(transferId)
    const fileStream = await fs.open(path, 'r')
    const stream = fileStream.createReadStream()
    stream.on('close', () => fileStream.close())
    return { stream }
  }

  async prepareZipBundleArchive(transferId, files, stream) {
    let aborted = false
    const archive = archiver('zip', { forceZip64: true, store: true })
      .on('error', err => aborted ? console.warn('client aborted') : console.error(err))
      .on("warning", warn => console.warn("Archiver warning:", warn))

    pipeline(archive, stream)
    stream.once('close', () => { aborted = true })

    for (const f of files) {
      console.log("getTransferFileKey:", f.name)
      const filePath = await this.getTransferFileKey(transferId, f.id)
      console.log("getObject:", f.name)

      const readStream = _fs.createReadStream(filePath)
      try {
        console.log("append:", f.name)
        archive.append(readStream, { name: f.relativePath })
      } finally {
        console.log("waiting:", f.name)
        await finished(readStream)
      }
    }
    archive.finalize()
  }

  async delete(transferId) {
    const dirPath = this.getTransferBaseKey(transferId)
    try {
      console.log("RM:", dirPath)
      const entries = await fs.readdir(dirPath, { withFileTypes: true })
      const forbiddenFiles = entries.filter(entry => {
        if (!entry.isFile()) return false
        const ext = path.extname(entry.name)
        return ext !== '.json' && ext !== ''
      })
      if (forbiddenFiles.length > 0) {
        console.error(`Delete refused: directory ${dirPath} contains forbidden files:`)
        forbiddenFiles.forEach(f => console.error(` - ${f.name}`))
        return { ok: false, error: 'Directory contains forbidden files' }
      }
      await fs.rm(dirPath, { recursive: true, force: true })
      return { ok: true }
    } catch (err) {
      if (err.code === 'ENOENT') return { ok: true }
      throw err
    }
  }
}

