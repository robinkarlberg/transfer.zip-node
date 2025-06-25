import { BaseProvider } from "./BaseProvider.js";
import fs from "fs/promises"

export class LocalProvider extends BaseProvider {
  constructor(config) {
    super(config)
  }

  getRootKey() {
    return this.config.root
  }

  async hasBundle(transferId) {
    try {
      const path = this.getBundleKey(transferId)
      const stat = await fs.stat(path);
      return stat.isFile()
    } catch (err) {
      if (err.code === 'ENOENT') return false
      throw err;
    }
  }
}