import { deleteKeyRecurse } from "../s3.js";
import { BaseUploader } from "../uploader/BaseUploader.js";
import validateFileId, { parseMeta } from "./providerUtils.js";

export class BaseProvider {
  constructor(config, uploader) {
    this.config = config;

    /** @type {BaseUploader} */
    this.uploader = uploader;
  }

  async init() {
    
  }

  async hasBundle(transferId) {
    throw new Error("must be implemented in subclass");
  }

  getRootKey() {
    throw new Error("must be implemented in subclass");
  }

  getTransferBaseKey(transferId) {
    return `${this.getRootKey()}${transferId}`
  }

  getBundleKey(transferId) {
    return `${this.getTransferBaseKey(transferId)}/bundle`
  }

  async getTransferFilesBaseKey(transferId) {
    if (await this.hasBundle()) {
      throw new Error("No files base key exists if bundle is finished.")
    }
    return `${this.getTransferBaseKey(transferId)}/files`
  }

  async getTransferFileKey(transferId, fileId) {
    if (await this.hasBundle()) {
      throw new Error("No files base key exists if bundle is finished.")
    }
    return `${this.getTransferBaseKey(transferId)}/files/${fileId}`
  }

  async listFiles(transferId) {
    if (await this.hasBundle()) {
      throw new Error("Can't list files if bundle is finished.")
    }
  }

  async createZipBundle(transferId, filesList) {
    throw new Error("must be implemented in subclass");
  }

  async prepareBundleSaved(transferId, fileName) {
    throw new Error("must be implemented in subclass");
  }

  async prepareZipBundleArchive(transferId, filesList) {
    throw new Error("must be implemented in subclass");
  }

  async presignUpload(transferId, fileId, fileSize) {
    const key = fileId ? this.getTransferFileKey(transferId, fileId) : this.getBundleKey(transferId)

    return await this.uploader.prepare({
      key,
      size: fileSize
    })
  }

  async namingFunction(req) {
    const meta = parseMeta(req.headers.get("upload-metadata"))

    if (!validateFileId(meta.id)) {
      throw new Error('Invalid fileId')
    }

    if(!req.node.req.auth) {
      throw new Error('no auth')
    }

    const { tid, filesCount } = req.node.req.auth

    if (filesCount == 1) {
      return this.getBundleKey(tid)
    }
    else {
      return await this.getTransferFileKey(tid, meta.id)
    }
  }

  async delete(transferId) {
    throw new Error("not impl")
  }
}