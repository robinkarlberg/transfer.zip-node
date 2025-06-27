import {
  PutObjectCommand,
  GetObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  ListBucketsCommand,
  DeleteObjectsCommand,
  DeleteObjectCommand,
  PutBucketLifecycleConfigurationCommand,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

// export async function signUpload({ client, bucket, key, type, maxAge = 3600 }) {
//   return getSignedUrl(
//     client,
//     new PutObjectCommand({
//       Bucket: bucket,
//       Key: key,
//       ContentType: type,        // e.g. "image/png"
//       ACL: 'private',           // R2 ignores ACL but keeps header for S3 parity
//     }),
//     { expiresIn: maxAge },      // seconds (1 s → 7 days)
//   );
// }

export async function signDownload({ client, bucket, key, fileName, maxAge = 600 }) {
  return getSignedUrl(
    client,
    new GetObjectCommand({
      Bucket: bucket, Key: key,
      ResponseContentDisposition: fileName ? `attachment; filename="${fileName}"` : undefined
    }),
    { expiresIn: maxAge },
  );
}

// export async function createMultipart(client, bucket, key) {
//   const { UploadId } = await client.send(
//     new CreateMultipartUploadCommand({ Bucket: bucket, Key: key })
//   );
//   return UploadId;
// }

// export async function presignPart(client, bucket, key, uploadId, partNumber, expires = 900) {
//   return getSignedUrl(
//     client,
//     new UploadPartCommand({
//       Bucket: bucket,
//       Key: key,
//       UploadId: uploadId,
//       PartNumber: partNumber,
//     }),
//     { expiresIn: expires },
//   );
// }

// export async function completeMultipart(client, bucket, key, uploadId, parts) {
//   return client.send(
//     new CompleteMultipartUploadCommand({
//       Bucket: bucket,
//       Key: key,
//       UploadId: uploadId,
//       MultipartUpload: { Parts: parts }, // [{PartNumber, ETag}, …]
//     }),
//   );
// }

// export async function abortMultipart(client, bucket, key, uploadId) {
//   return client.send(
//     new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId }),
//   );
// }

export async function headBucket(client, bucket, key) {
  return client.send(new HeadObjectCommand({
    Bucket: bucket,
    Key: key
  }));
}

export async function listAllObjects(client, bucket, prefix) {
  const objects = []
  let token

  do {
    const resp = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: token,
        MaxKeys: 1000,
      })
    )
    resp.Contents?.forEach(o =>
      objects.push({ key: o.Key, size: o.Size })
    )
    token = resp.IsTruncated ? resp.NextContinuationToken : undefined
  } while (token)

  return objects
}

export async function getObject(client, bucket, key) {
  return client.send(
    new GetObjectCommand({ Bucket: bucket, Key: key })
  )
}

export async function listBuckets(client) {
  return client.send(new ListBucketsCommand());
}

export async function deleteKeyRecurse(client, bucket, key) {
  const objects = await listAllObjects(client, bucket, key)
  const keys = objects.map(obj => ({ Key: obj.key }))
  if (keys.length === 0) return

  // S3 allows up to 1000 keys per delete
  for (let i = 0; i < keys.length; i += 1000) {
    await client.send(
      new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: {
          Objects: keys.slice(i, i + 1000),
          Quiet: true
        }
      })
    )
  }

  // Also delete the prefix `key` if it is itself an object
  if (!objects.some(obj => obj.key === key)) {
    try {
      await client.send(
        new DeleteObjectCommand({
          Bucket: bucket,
          Key: key
        })
      )
    } catch (e) {
      // ignore if not found
    }
  }
}

export async function deleteKey(client, bucket, key) {
  return client.send(
    new DeleteObjectCommand({
      Bucket: bucket,
      Key: key
    })
  )
}

export async function setAbortMultipartLifecycle(client, bucketName) {
  return client.send(
    new PutBucketLifecycleConfigurationCommand({
      Bucket: bucketName,
      LifecycleConfiguration: {
        Rules: [
          {
            ID: "AbortIncompleteMultipartUploadsAfter2Days",
            Status: "Enabled",
            AbortIncompleteMultipartUpload: {
              DaysAfterInitiation: 2
            },
            Filter: {
              Prefix: ""
            }
          }
        ]
      }
    })
  )
}