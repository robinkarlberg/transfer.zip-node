import { BaseUploader } from "./BaseUploader.js";

export class TusUploader extends BaseUploader {
  constructor ({ chunkSize }) {
    super();
    this.endpoint  = "/upload/tus";
    this.chunkSize = chunkSize;
  }

  async prepare () {
    return {
      protocol:  'tus',
      endpoint:  this.endpoint,
      chunkSize: this.chunkSize,     // <— client can pass to Uppy/Tus plugin
    };
  }
}