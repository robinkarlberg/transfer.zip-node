import { deleteKeyRecurse } from "../s3.js";
import { BaseUploader } from "../uploader/BaseUploader.js";
import { validateFileId } from "./providerUtils.js";

export class BaseProvider {
  constructor(config, uploader) {
    this.config = config;

    /** @type {BaseUploader} */
    this.uploader = uploader;
  }

  async init() {
    if (this.datastore && this.datastore.init) {
      await this.datastore.init()
    }
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

  getTransferFilesBaseKey(transferId) {
    return `${this.getTransferBaseKey(transferId)}/files`
  }

  // Per-file objects stay in the bucket after the bundle is zipped —
  // previews and per-file downloads rely on that.
  getTransferFileKey(transferId, fileId) {
    return `${this.getTransferBaseKey(transferId)}/files/${fileId}`
  }

  getPreviewsBaseKey(transferId) {
    return `${this.getTransferBaseKey(transferId)}/previews`
  }

  // fileIds are 24-hex, so these names can't collide with each other
  getThumbKey(transferId, fileId) {
    return `${this.getPreviewsBaseKey(transferId)}/thumb-${fileId}`
  }

  getPreviewKey(transferId, fileId) {
    return `${this.getPreviewsBaseKey(transferId)}/preview-${fileId}`
  }

  getPreviewsManifestKey(transferId) {
    return `${this.getPreviewsBaseKey(transferId)}/manifest`
  }

  // Per-file presigned URLs + thumbnails. Off unless the subclass can presign.
  supportsPreviews() {
    return false
  }

  async createPreviews(transferId, filesList, logger) {
    throw new Error("must be implemented in subclass");
  }

  async getPreviewsManifest(transferId) {
    throw new Error("must be implemented in subclass");
  }

  async signFileDownloads(transferId, filesCount, files) {
    throw new Error("must be implemented in subclass");
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

  async prepareZipBundleArchive(transferId, filesList, logger) {
    throw new Error("must be implemented in subclass");
  }

  async namingFunction(req, metadata) {
    if (!validateFileId(metadata.id)) {
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
      return await this.getTransferFileKey(tid, metadata.id)
    }
  }

  async delete(transferId) {
    throw new Error("not impl")
  }
}