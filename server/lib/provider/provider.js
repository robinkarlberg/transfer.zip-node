import { conf } from "../config.js";
import { LocalProvider } from "./LocalProvider.js";
import { S3Provider } from "./S3Provider.js";

// legacy provider to facilitate downloads of old v1 transfers
// TODO: remove this when pushing to stable self-host (main) branch
console.log("backend_v1_temp_dl", conf.backend_v1_temp_dl ? "ENABLED" : "DISABLED")
export const legacyProvider = !!conf.backend_v1_temp_dl ? new S3Provider(conf.providers[conf.backend_v1_temp_dl]) : null

export const provider = conf.active == "local" ?
  new LocalProvider(conf.providers["local"])
  : new S3Provider(conf.providers[conf.active])

