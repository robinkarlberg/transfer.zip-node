import { conf } from "../config.js";
import { LocalProvider } from "./LocalProvider.js";
import { S3Provider } from "./S3Provider.js";


export const provider = conf.active == "local" ?
  new LocalProvider(conf.providers["local"])
  : new S3Provider(conf.providers[conf.active])

