import { Buffer } from 'node:buffer';

export default function validateFileId(fileId) {
  if (!/^[a-fA-F0-9]{24}$/.test(fileId)) {
    return false
  }
  return true
}

export function parseMeta(header) {
  console.log("header:", header)
  const obj = {};
  header.split(',').forEach(pair => {
    const [k, v] = pair.trim().split(' ');
    if (k) obj[k] = Buffer.from(v, 'base64').toString();
  });
  return obj;
}