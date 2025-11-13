import { conf } from "../config.js";
import { LocalProvider } from "./LocalProvider.js";
import { S3Provider } from "./S3Provider.js";

// legacy provider to facilitate downloads of old v1 transfers
// TODO: remove this when pushing to stable self-host (main) branch
export const legacyProvider = new S3Provider(conf.providers[conf.backend_v1_temp_dl])

export const provider = conf.active == "local" ?
  new LocalProvider(conf.providers["local"])
  : new S3Provider(conf.providers[conf.active])

