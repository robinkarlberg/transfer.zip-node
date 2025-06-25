export class BaseUploader {
  /** Return an object the browser can feed to its upload plugin. */
  async prepare({ key, size }) {
    throw new Error('must be implemented');
  }
}