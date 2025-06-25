import { createMultipart, signUpload } from "../s3.js";
import { BaseUploader } from "./BaseUploader.js";

export class S3PresignUploader extends BaseUploader {
  constructor({ bucket, client, maxSingle = 5 * 1024 ** 3 }) {
    super();
    this.bucket = bucket;
    this.client = client;
    this.maxSingle = maxSingle;
  }

  /** Decide single-PUT vs multipart and return the JSON that
      @uppy/aws-s3 expects (method/url OR { uploadId, … }). */
  async prepare({ key, size }) {
    // single request path
    if (size <= this.maxSingle) {
      const url = await signUpload({
        client: this.client,
        bucket: this.bucket,
        key,
        type: "application/octet-stream"
      })

      return {                // exactly what Uppy’s getUploadParameters() wants
        protocol: 's3',
        method: 'PUT',
        url,
        // headers: { 'content-type': mime },
      };
    }

    // —— multipart path ————————————————————————————
    const { UploadId } = await createMultipart(this.client, this.bucket, key)
    return {
      protocol: 's3-multipart',
      uploadId: UploadId,
      key,
      bucket: this.bucket,
      // the browser will call /sign again for each partNumber
    };
  }

  /** Optional helpers for /sign, /complete, /abort routes ... */
  async signPart({ key, uploadId, partNumber }) { /* … */ }
  async complete({ key, uploadId, parts }) { /* … */ }
  async abort({ key, uploadId }) { /* … */ }
}