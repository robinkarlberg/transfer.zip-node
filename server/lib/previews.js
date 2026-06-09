// Keep these in sync with PREVIEWABLE_IMAGE_TYPES / MAX_PREVIEWABLE_IMAGE_BYTES
// in transfer.zip-web (next/src/lib/transferUtils.js).
export const PREVIEWABLE_IMAGE_TYPES = [
  "image/jpeg",
  "image/jpg",
  "image/png",
  "image/webp",
  "image/gif",
  "image/avif",
  "image/svg+xml",
  "image/tiff",
]

export const MAX_PREVIEWABLE_IMAGE_BYTES = 20 * 1024 * 1024

export const PREVIEW_CACHE_CONTROL = "public, max-age=604800, immutable"
export const PREVIEW_URL_MAX_AGE = 6 * 3600
export const ORIGINAL_URL_MAX_AGE = 3600

export function isPreviewableFileType(type) {
  if (!type) return false
  return PREVIEWABLE_IMAGE_TYPES.includes(type.toLowerCase())
}

export function isPreviewableFile(file) {
  return isPreviewableFileType(file.type) && (file.size || 0) <= MAX_PREVIEWABLE_IMAGE_BYTES
}
