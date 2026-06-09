import sharp from "sharp"

const THUMB_BOX = 512
const PREVIEW_BOX = 1600

// Webp variants for the download page: `thumb` for the gallery grid,
// `preview` for the lightbox. Buffer input is fine — sources are capped
// at MAX_PREVIEWABLE_IMAGE_BYTES. `rotate()` applies EXIF orientation.
export async function generateImageVariants(buffer) {
  const [thumb, preview] = await Promise.all([
    sharp(buffer)
      .rotate()
      .resize({ width: THUMB_BOX, height: THUMB_BOX, fit: "inside", withoutEnlargement: true })
      .webp({ quality: 75 })
      .toBuffer(),
    sharp(buffer)
      .rotate()
      .resize({ width: PREVIEW_BOX, height: PREVIEW_BOX, fit: "inside", withoutEnlargement: true })
      .webp({ quality: 80 })
      .toBuffer(),
  ])
  return { thumb, preview }
}
