import { Buffer } from 'node:buffer';

export function validateFileId(fileId) {
  if (!/^[a-fA-F0-9]{24}$/.test(fileId)) {
    return false
  }
  return true
}
