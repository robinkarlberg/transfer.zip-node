import archiver from "archiver"
import { deleteKeyRecurse, getObject, headBucket, listAllObjects, listBuckets, setAbortMultipartLifecycle, signDownload } from "../s3.js";
import { BaseProvider } from "./BaseProvider.js";
import { Upload } from "@aws-sdk/lib-storage";
import { conf } from "../config.js";
import { S3Client } from "@aws-sdk/client-s3";
import { S3Store } from "@tus/s3-store";
import { PassThrough } from "stream";
import { finished, pipeline } from "stream/promises";
import Bottleneck from "bottleneck";
import { DiskCacheS3Store } from "../store/S3DiskCacheS3Store.js";

export class S3Provider extends BaseProvider {
  constructor(config) {
    super(config)
    this.client = new S3Client(this.config.s3)
    this.datastore = new DiskCacheS3Store({
      s3ClientConfig: {
        endpoint: this.config.s3.endpoint,
        region: this.config.s3.region,
        credentials: this.config.s3.credentials,
        bucket: this.config.bucket,
      },
      partSize: this.config.partSizeMB * 1024 ** 2,
      queueSize: this.config.parallelWrites,
      maxConcurrentPartUploads: 8,
    })

    // this.client.config.credentials().then(console.log)
    // listBuckets(this.client).then(console.log)
    // listAllObjects(this.client, "kb-dev-0", "/").then(console.log)
    // console.log(this.config.s3)
    // console.log(this.client)
  }

  async init() {
    await setAbortMultipartLifecycle(this.client, this.config.bucket)
  }

  getRootKey() {
    return ``
  }

  /** TODO: Cache this lol */
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
      partSize: this.config.partSizeMB * 1024 ** 2,
      leavePartsOnError: false
    })

    this.prepareZipBundleArchive(transferId, filesList, passThrough)

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
    let aborted = false
    const archive = archiver('zip', { forceZip64: true, store: true })
      .on('error', err => aborted ? console.warn('client aborted') : console.error(err))
      .on("warning", warn => console.warn("Archiver warning:", warn))

    pipeline(archive, stream)
    stream.once('close', () => { aborted = true })

    for (const f of files) {
      console.log("getTransferFileKey:", f.name)
      const key = await this.getTransferFileKey(transferId, f.id);
      console.log("getObject:", f.name)
      const { Body } = await getObject(this.client, this.config.bucket, key);

      console.log("append:", f.name)
      archive.append(Body, { name: f.relativePath });
      console.log("waiting:", f.name)
      await finished(Body)
    }
    archive.finalize()
  }

  async delete(transferId) {
    return deleteKeyRecurse(this.client, this.config.bucket, this.getTransferBaseKey(transferId))
  }
}