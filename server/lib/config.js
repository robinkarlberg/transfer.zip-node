import { readFileSync } from "fs";

export const conf = JSON.parse(readFileSync("./conf.json"))