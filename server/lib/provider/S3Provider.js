import archiver from "archiver"
import { abortMultipart as s3AbortMultipart, completeMultipart, createMultipart, deleteKeyRecurse, getObject, headBucket, listAllObjects, listBuckets, putObject, setAbortMultipartLifecycle, setBucketCors, signDownload, signPart, signUpload } from "../s3.js";
import { isPreviewableFile, ORIGINAL_URL_MAX_AGE, PREVIEW_CACHE_CONTROL, PREVIEW_URL_MAX_AGE } from "../previews.js";
import { BaseProvider } from "./BaseProvider.js";
import { Upload } from "@aws-sdk/lib-storage";
import { conf } from "../config.js";
import { S3Client } from "@aws-sdk/client-s3";
import { S3Store } from "@tus/s3-store";
import { PassThrough } from "stream";
import { finished, pipeline } from "stream/promises";
import Bottleneck from "bottleneck";
import { DiskCacheS3Store } from "../store/S3DiskCacheS3Store.js";
import pino from "pino";

export class S3Provider extends BaseProvider {
  constructor(config) {
    super(config)
    this.client = new S3Client({
      ...this.config.s3,
      // Backstop against socket leaks: requestTimeout is a socket
      // *inactivity* timeout, so it reaps stalled/leaked streams without
      // affecting slow-but-moving transfers.
      requestHandler: {
        connectionTimeout: 5_000,
        requestTimeout: 300_000,
        httpsAgent: { keepAlive: true, maxSockets: 1000 },
        httpAgent: { keepAlive: true, maxSockets: 1000 },
      },
    })
    // this.datastore = new DiskCacheS3Store({
    //   s3ClientConfig: {
    //     endpoint: this.config.s3.endpoint,
    //     region: this.config.s3.region,
    //     credentials: this.config.s3.credentials,
    //     bucket: this.config.bucket,
    //   },
    //   partSize: this.config.partSizeMB * 1024 ** 2,
    //   queueSize: this.config.parallelWrites,
    //   maxConcurrentPartUploads: 8,
    // })

    // this.client.config.credentials().then(console.log)
    // listBuckets(this.client).then(console.log)
    // listAllObjects(this.client, "kb-dev-0", "/").then(console.log)
    // console.log(this.config.s3)
    // console.log(this.client)
  }

  async signUpload(transferId, filesCount, fileId) {
    const key = filesCount == 1 ? this.getBundleKey(transferId) : await this.getTransferFileKey(transferId, fileId)
    return signUpload({
      client: this.client,
      bucket: this.config.bucket,
      key
    })
  }

  async createMultipart(transferId, filesCount, fileId) {
    const key = filesCount == 1 ? this.getBundleKey(transferId) : await this.getTransferFileKey(transferId, fileId)
    return createMultipart({
      client: this.client,
      bucket: this.config.bucket,
      key
    })
  }

  async signPart(transferId, filesCount, fileId, uploadId, partNumber) {
    const key = filesCount == 1 ? this.getBundleKey(transferId) : await this.getTransferFileKey(transferId, fileId)
    return signPart({
      client: this.client,
      bucket: this.config.bucket,
      key,
      uploadId,
      partNumber
    })
  }

  async completeMultipart(transferId, filesCount, fileId, uploadId, parts) {
    const key = filesCount == 1 ? this.getBundleKey(transferId) : await this.getTransferFileKey(transferId, fileId)
    return completeMultipart({
      client: this.client,
      bucket: this.config.bucket,
      key,
      uploadId,
      parts
    })
  }

  async abortMultipart(transferId, filesCount, fileId, uploadId) {
    const key = filesCount == 1 ? this.getBundleKey(transferId) : await this.getTransferFileKey(transferId, fileId)
    return s3AbortMultipart({
      client: this.client,
      bucket: this.config.bucket,
      key,
      uploadId
    })
  }

  async init() {
    await setAbortMultipartLifecycle(this.client, this.config.bucket)
    await setBucketCors(this.client, this.config.bucket)
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

  async createZipBundle(transferId, filesList, logger = console) {
    logger.info(`Creating zip bundle...`)
    const passThrough = new PassThrough()

    const uploader = new Upload({
      client: this.client,
      params: { Bucket: this.config.bucket, Key: this.getBundleKey(transferId), Body: passThrough },
      queueSize: this.config.parallelWrites,
      partSize: this.config.partSizeMB * 1024 ** 2,
      leavePartsOnError: false
    })

    const zipping = this.prepareZipBundleArchive(transferId, filesList, passThrough, logger)
    const uploading = uploader.done()

    try {
      await Promise.all([zipping, uploading])
    } catch (err) {
      // Kill whichever side is still alive and wait for both to wind down,
      // so the BullMQ retry starts with nothing left running.
      passThrough.destroy(err)
      await Promise.allSettled([zipping, uploading])
      throw err
    }

    logger.info(`Zip bundle finished!`)
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

  /**
   *
   * @param {*} transferId
   * @param {*} files
   * @param {*} stream
   * @param {pino.Logger} logger
   */
  async prepareZipBundleArchive(transferId, files, stream, logger = console) {
    logger.info(`Archiver starting...`)
    const archive = archiver('zip', { forceZip64: true, store: true })
      // listener must stay attached: archiver can emit 'error' after the
      // pipeline has settled (e.g. append after abort)
      .on('error', err => logger.warn(`archiver error: ${err.message}`))
      .on("warning", warn => logger.warn(warn, "archiver warning"))

    const archiveDone = pipeline(archive, stream)
    let pipelineErr = null
    archiveDone.catch(err => { pipelineErr = err })

    let current = null
    try {
      for (const f of files) {
        if (pipelineErr) throw pipelineErr

        const key = await this.getTransferFileKey(transferId, f.id);

        let Body;
        try {
          ({ Body } = await getObject(this.client, this.config.bucket, key));
        }
        catch (err) {
          logger.error(`Failed to get object: ${key}`)
          logger.error(err)
          continue
        }
        current = Body
        // TODO: check if "f.relativePath || f.name" fucks anything up
        const fileFullName = f.relativePath || f.name
        archive.append(Body, { name: fileFullName });
        logger.debug(`Archiver now waiting for: ${fileFullName}`)
        // settles when the entry is fully read, or as soon as the
        // archive/destination dies — whichever comes first
        await Promise.race([finished(Body), archiveDone])
        current = null
      }
      await Promise.race([archive.finalize(), archiveDone])
      await archiveDone
      logger.info(`Archiver finished!`)
    } catch (err) {
      // destroy the in-flight S3 stream so its socket is released
      current?.destroy(err)
      archive.destroy()
      throw err
    }
  }

  supportsPreviews() {
    return true
  }

  async createPreviews(transferId, filesList, logger = console) {
    // sharp is loaded lazily so a missing/broken native install can't take
    // down server boot — preview jobs fail and retry instead
    const { generateImageVariants } = await import("../previewGenerator.js")

    const filesCount = filesList.length
    const generated = {}

    for (const f of filesList) {
      if (!isPreviewableFile(f)) continue

      const sourceKey = filesCount == 1 ? this.getBundleKey(transferId) : this.getTransferFileKey(transferId, f.id)
      try {
        const res = await getObject(this.client, this.config.bucket, sourceKey)
        const source = Buffer.from(await res.Body.transformToByteArray())
        const { thumb, preview } = await generateImageVariants(source)

        await putObject(this.client, this.config.bucket, this.getThumbKey(transferId, f.id), thumb, {
          contentType: "image/webp",
          cacheControl: PREVIEW_CACHE_CONTROL
        })
        await putObject(this.client, this.config.bucket, this.getPreviewKey(transferId, f.id), preview, {
          contentType: "image/webp",
          cacheControl: PREVIEW_CACHE_CONTROL
        })
        generated[f.id] = { thumb: true, preview: true }
      } catch (err) {
        // an undecodable image shouldn't fail the whole job — that file
        // simply won't get previews
        logger.warn(`Preview generation failed for file ${f.id}: ${err.message}`)
      }
    }

    await putObject(this.client, this.config.bucket, this.getPreviewsManifestKey(transferId), JSON.stringify({ v: 1, files: generated }), {
      contentType: "application/json"
    })

    logger.info(`Previews finished: ${Object.keys(generated).length}/${filesList.length} files`)
    return { ok: true }
  }

  async getPreviewsManifest(transferId) {
    try {
      const res = await getObject(this.client, this.config.bucket, this.getPreviewsManifestKey(transferId))
      return JSON.parse(await res.Body.transformToString())
    } catch (err) {
      if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
        return null
      }
      throw err
    }
  }

  async signFileDownloads(transferId, filesCount, files) {
    // null manifest = previews not generated (yet) — sign originals only
    const manifest = await this.getPreviewsManifest(transferId)

    const out = {}
    for (const f of files) {
      const originalKey = filesCount == 1 ? this.getBundleKey(transferId) : this.getTransferFileKey(transferId, f.id)
      const entry = {
        original: await signDownload({
          client: this.client,
          bucket: this.config.bucket,
          key: originalKey,
          fileName: f.name,
          maxAge: ORIGINAL_URL_MAX_AGE
        })
      }

      const variants = manifest?.files?.[f.id]
      if (variants?.thumb) {
        entry.thumb = await signDownload({
          client: this.client,
          bucket: this.config.bucket,
          key: this.getThumbKey(transferId, f.id),
          maxAge: PREVIEW_URL_MAX_AGE
        })
      }
      if (variants?.preview) {
        entry.preview = await signDownload({
          client: this.client,
          bucket: this.config.bucket,
          key: this.getPreviewKey(transferId, f.id),
          maxAge: PREVIEW_URL_MAX_AGE
        })
      }

      out[f.id] = entry
    }
    return out
  }

  async delete(transferId) {
    return deleteKeyRecurse(this.client, this.config.bucket, this.getTransferBaseKey(transferId))
  }
}