import cors from '@fastify/cors'
import fastifyFormbody from '@fastify/formbody'
import fastifyJwt from '@fastify/jwt'
import fastifySensible from '@fastify/sensible'
import { Job } from 'bullmq'
import Fastify from 'fastify'
import { readFileSync } from 'node:fs'
import { PassThrough } from 'node:stream'
import { randomHttpErrorInDev } from './lib/dev/randomError.js'
import { legacyProvider, provider } from './lib/provider/provider.js'
import zipperQueue from './lib/queue/zipperQueue.js'
import startWorker from './lib/queue/zipperWorker.js'
import { existsSync } from 'node:fs'
import pino from 'pino'

const PINO_CONF = {
  level: "info",
  formatters: {
    level: (label) => {
      return { level: label };
    },
  },
}

export const logger = pino(PINO_CONF)

const app = Fastify({
  logger: PINO_CONF,
  requestTimeout: 0
})
app.register(fastifySensible)

const pubKeyPath =
  process.env.NODE_ENV === 'development'
    ? '../_local_dev_keys/public.pem'
    : '/keys/public.pem'

if (!existsSync(pubKeyPath)) {
  console.log("Couldn't find public key! See https://github.com/robinkarlberg/transfer.zip-web/blob/main/SELFHOSTING.md")
  await new Promise(r => setTimeout(r, 5000))
  process.exit(1)
}

const pubKey = readFileSync(pubKeyPath)

app.register(cors, {
  origin: true, // Allow all origins
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['*', "Authorization"],
})

app.register(fastifyJwt, {
  secret: { public: pubKey },
  verify: { algorithms: ['RS256'], audience: 'transfer.zip' }
})

async function validateToken(token, requiredScope) {
  if (!token) throw new Error('Missing token');

  let payload;
  try {
    payload = await app.jwt.verify(token, {
      algorithms: ['RS256'],
      audience: 'transfer.zip'
    });
  } catch {
    throw new Error('Invalid token');
  }

  if (requiredScope && !payload.scope?.split(' ').includes(requiredScope)) {
    throw new Error("forbidden");
  }

  // TODO: Fix this fucking shit cause gpt is a fucking idiot
  // if (
  //   contentLength != null &&
  //   payload.maxSize != null &&
  //   +contentLength > +payload.maxSize
  // ) {
  //   throw new Error("429");
  // }

  return payload;
}

function needsScope(requiredScope, getTokenFromBody) {
  return async req => {
    const token = getTokenFromBody
      ? req.body?.token
      : (req.headers.authorization || '').replace(/^Bearer\s+/i, '');

    const payload = await validateToken(token, requiredScope);

    req.auth = payload;
    req.raw.auth = payload; // for tus
  };
}

// app.addContentTypeParser(
//   "application/offset+octet-stream",
//   (request, payload, done) => done(null)
// );

const handleDownload = async (req, reply) => {
  const { tid, size, filesCount, name, backendVersion } = req.auth
  const chosenProvider = backendVersion == 2 ? provider : legacyProvider

  let hasBundle = null

  /** @type {Job} */
  const zipperJob = await zipperQueue.getJob(tid)
  if (zipperJob) {
    const state = await zipperJob.getState()
    if (["active", "delayed", "failed", "waiting"].includes(state)) {
      // Doesn't have bundle if job is in this state
      hasBundle = false
    }
  }

  // If hasBundle hasnt been set to false with the zipperJob check
  if (hasBundle === null) {
    // Maybe has a bundle
    hasBundle = await chosenProvider.hasBundle(tid)
  }

  if (hasBundle) {
    // Returns either stream with fileType, or a download url
    const { url, stream, fileType } = await chosenProvider.prepareBundleSaved(tid, name)

    if (url) {
      reply.redirect(url)
    }
    else if (stream) {
      if (fileType) {
        reply.header('Content-Type', fileType)
      }
      reply.header('Content-Disposition', `attachment; filename="${name}"`)

      reply.send(stream)
      // if (archive) {
      //   archive.finalize()
      // }
    }
  }
  else {
    if (!zipperJob) {
      throw new Error(`Transfer ${tid} does not have a bundle, and does not have a zipper job. Something is wrong! We can not get the file data if the job can't be found.`)
    }

    const { filesList } = zipperJob.data

    reply.header('Content-Type', "application/zip")
    reply.header('Content-Disposition', `attachment; filename="${name}"`)

    const passThrough = new PassThrough()
    reply.send(passThrough)
    await chosenProvider.prepareZipBundleArchive(tid, filesList, passThrough, logger.child({ action: "ondemand-zipper", transferId: tid }))
  }
}

const handleControlTransferStatus = async (req) => {
  const { transferId, backendVersion } = req.body

  const chosenProvider = backendVersion == 2 ? provider : legacyProvider

  const hasZipBundle = await chosenProvider.hasBundle(transferId)
  return { hasZipBundle }
}

const handleControlTransferDelete = async (req) => {
  const { transferId, backendVersion } = req.body
  const chosenProvider = backendVersion == 2 ? provider : legacyProvider
  // TODO: Handle edge cases when zipper job is active or waiting
  // try {
  //   const zipperJob = await zipperQueue.getJob(transferId)
  //   if (zipperJob) {
  //     const state = await zipperJob.getState()
  //     if (state === 'active') {
  //       await zipperJob.moveToFailed(new Error('Job stopped because'), true)
  //     }
  //     await zipperJob.remove()
  //   }
  // }
  // catch (err) {
  //   console.error("Failed to stop zipper job:", err)
  // }
  req.log.info(`Deleting transfer: ${transferId} Backend version: ${backendVersion}`)
  await chosenProvider.delete(transferId)
  req.log.info(`Deleted transfer: ${transferId} Backend version: ${backendVersion}`)

  return { success: true }
}

const handleControlUploadComplete = async (req) => {
  const { transferId, filesList } = req.body

  const willZip = filesList.length > 1
  req.log.info(`Upload complete: ${transferId} Files: ${filesList.length} Will zip: ${willZip}`)
  // If there are more than one file, it should be zipped into the bundle
  // If there is only one file, the bundle IS that file already (to avoid zipping one file)
  if (willZip) {
    const totalSize = filesList.reduce((sum, file) => sum + (file.size || 0), 0)
    // console.log(
    //   "Adding to zipperQueue:",
    //   transferId,
    //   `${filesList.length} files`,
    //   `total size: ${totalSize} bytes`,
    //   filesList
    // )
    await zipperQueue.add(`${transferId}-zipper`, { filesList }, {
      jobId: transferId,
      attempts: 10,
      backoff: {
        type: "exponential",
        delay: 1000
      }
    })
  }

  return { success: true }
}

app.register(async function (app) {
  await app.register(fastifyFormbody)

  app.post('/download', { preHandler: needsScope('download', true) }, (req, reply) => {
    handleDownload(req, reply)
  })
})

app.route({
  method: ['OPTIONS', 'HEAD', 'POST', 'PATCH'],
  url: '/upload/sign',
  preHandler: needsScope('upload'),
  handler: async (req, reply) => {
    randomHttpErrorInDev(0.1)
    const { fileId } = req.body
    if (!fileId || !/^[0-9a-fA-F]{24}$/.test(fileId)) {
      return reply.badRequest('Invalid fileId')
    }

    const { tid, size, filesCount } = req.auth
    const url = await provider.signUpload(tid, filesCount, fileId)
    reply.send({ url })
  }
})

app.route({
  method: ['OPTIONS', 'HEAD', 'POST', 'PATCH'],
  url: '/upload/multipart/create',
  preHandler: needsScope('upload'),
  handler: async (req, reply) => {
    randomHttpErrorInDev(0.1)
    const { fileId } = req.body
    if (!fileId || !/^[0-9a-fA-F]{24}$/.test(fileId)) {
      return reply.badRequest('Invalid fileId')
    }

    const { tid, size, filesCount } = req.auth
    const uploadId = await provider.createMultipart(tid, filesCount, fileId)
    reply.send({ uploadId })
  }
})

app.route({
  method: ['OPTIONS', 'HEAD', 'POST', 'PATCH'],
  url: '/upload/multipart/sign-part',
  preHandler: needsScope('upload'),
  handler: async (req, reply) => {
    randomHttpErrorInDev(0.1)
    const { fileId, uploadId, partNumber } = req.body
    if (!fileId || !/^[0-9a-fA-F]{24}$/.test(fileId)) {
      return reply.badRequest('Invalid fileId')
    }

    const { tid, size, filesCount } = req.auth
    const url = await provider.signPart(tid, filesCount, fileId, uploadId, partNumber)
    reply.send({ url })
  }
})

app.route({
  method: ['OPTIONS', 'HEAD', 'POST', 'PATCH'],
  url: '/upload/multipart/complete',
  preHandler: needsScope('upload'),
  handler: async (req, reply) => {
    randomHttpErrorInDev(0.1)
    const { fileId, uploadId, parts } = req.body
    if (!fileId || !/^[0-9a-fA-F]{24}$/.test(fileId)) {
      return reply.badRequest('Invalid fileId')
    }

    const { tid, size, filesCount } = req.auth
    const res = await provider.completeMultipart(tid, filesCount, fileId, uploadId, parts)
    reply.send({ success: true })
  }
})

app.route({
  method: ['OPTIONS', 'HEAD', 'POST', 'PATCH'],
  url: '/upload/multipart/abort',
  preHandler: needsScope('upload'),
  handler: async (req, reply) => {
    randomHttpErrorInDev(0.1)
    const { fileId, uploadId, parts } = req.body
    if (!fileId || !/^[0-9a-fA-F]{24}$/.test(fileId)) {
      return reply.badRequest('Invalid fileId')
    }

    const { tid, size, filesCount } = req.auth
    const res = await provider.abortMultipart(tid, filesCount, fileId, uploadId)
    reply.send({ success: true })
  }
})

app.post('/control/transferStatus', { preHandler: needsScope('control') }, async (req, reply) => {
  return await handleControlTransferStatus(req, reply)
})

app.post('/control/transfer/delete', { preHandler: needsScope('control') }, async (req, reply) => {
  return await handleControlTransferDelete(req, reply)
})

app.post('/control/uploadComplete', { preHandler: needsScope('control') }, async (req, reply) => {
  return await handleControlUploadComplete(req, reply)
})

app.get('/ping', () => ({ success: true }))

// app.get("/robots.txt", () => )

process.on('uncaughtException', e => {
  logger.error(e)
})

process.on('unhandledRejection', e => {
  logger.error(e)
})

await provider.init()
startWorker(logger)
await app.listen({ port: 3050, host: process.env.NODE_ENV === "development" ? '127.0.0.1' : '0.0.0.0' })