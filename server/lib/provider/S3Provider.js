import archiver from "archiver"
import { deleteKeyRecurse, getObject, headBucket, listAllObjects, listBuckets, signDownload } from "../s3.js";
import { BaseProvider } from "./BaseProvider.js";
import { Upload } from "@aws-sdk/lib-storage";
import { conf } from "../config.js";
import { S3Client } from "@aws-sdk/client-s3";
import { S3Store } from "@tus/s3-store";
import validateFileId, { parseMeta } from "./providerUtils.js";
import { PassThrough } from "stream";
import { finished, pipeline } from "stream/promises";
import Bottleneck from "bottleneck";

export class S3Provider extends BaseProvider {
  constructor(config) {
    super(config)
    this.client = new S3Client(this.config.s3)
    // this.datastore here
    // new S3Client({
    //   endpoint
    // })
    this.datastore = new S3Store({
      s3ClientConfig: {
        endpoint: this.config.s3.endpoint,
        region: this.config.s3.region,
        credentials: this.config.s3.credentials,
        bucket: this.config.bucket,
      },
      partSize: conf.partSizeMB * 1024 ** 2,
      queueSize: this.config.parallelWrites,
    })

    // this.client.config.credentials().then(console.log)
    // listBuckets(this.client).then(console.log)
    // listAllObjects(this.client, "kb-dev-0", "/").then(console.log)
    // console.log(this.config.s3)
    // console.log(this.client)
  }

  getRootKey() {
    return ``
  }

  async hasBundle(transferId) {
    try {
      await headBucket(this.client, this.config.bucket, this.getBundleKey(transferId))
      return true
    } catch (err) {
      if (err.name === 'NotFound' || err.$metadata?.httpStatusCode === 404) {
        return false
      }
      throw err
    }
  }

  async listFiles(transferId) {
    super.listFiles()

    const prefix = await this.getTransferFilesBaseKey(transferId)

    const objects = await listAllObjects(this.client, this.config.bucket, prefix)

    return objects.map(object => ({ id: object.key, size: object.size }))
  }

  async createZipBundle(transferId, filesList) {
    const passThrough = new PassThrough()

    const uploader = new Upload({
      client: this.client,
      params: { Bucket: this.config.bucket, Key: this.getBundleKey(transferId), Body: passThrough },
      queueSize: this.config.parallelWrites,
      partSize: conf.partSizeMB * 1024 ** 2,
      leavePartsOnError: false
    })

    await this.prepareZipBundleArchive(transferId, filesList, passThrough)

    await uploader.done()

    return { ok: true }
  }

  async prepareBundleSaved(transferId, fileName) {
    const key = this.getBundleKey(transferId)
    const url = await signDownload({
      client: this.client,
      bucket: this.config.bucket,
      key,
      fileName
    })
    return { url }
  }

  async prepareZipBundleArchive(transferId, files, stream) {
    const STREAMS = 4
    const BYTE_WIN = 10 * 1024 * 1024       // 10 MB

    const streamLimiter = new Bottleneck({ maxConcurrent: STREAMS })
    const byteLimiter = new Bottleneck({ maxConcurrent: BYTE_WIN })
    let aborted = false
    const archive = archiver('zip', { forceZip64: true, store: true })
      .on('error', err => aborted ? console.warn('client aborted') : console.error(err))

    pipeline(archive, stream)
    stream.once('close', () => { aborted = true })

    const tasks = files.map(f =>
      streamLimiter.schedule(() =>
        byteLimiter.schedule({ weight: clampWeight(f.size, BYTE_WIN) }, async () => {
          const key = await this.getTransferFileKey(transferId, f.id)
          const { Body } = await getObject(this.client, this.config.bucket, key)

          archive.append(Body, { name: f.relativePath, size: f.size })
          await finished(Body)
        })
      ).finally(() =>               // give the tokens back
        byteLimiter.incrementReservoir(clampWeight(f.size, BYTE_WIN))
      )
    )

    await Promise.all(tasks)
    archive.finalize()
  }

  async delete(transferId) {
    return deleteKeyRecurse(this.client, this.config.bucket, this.getTransferBaseKey(transferId))
  }
}

function clampWeight(size, WINDOW) {
  return Math.min(size, WINDOW)
}