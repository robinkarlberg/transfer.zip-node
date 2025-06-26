import Fastify from 'fastify'
import fastifyJwt from '@fastify/jwt'
import { readFileSync } from 'node:fs'
import { Server as TusServer } from '@tus/server'
import { provider } from './lib/provider/provider.js'
import zipperQueue from './lib/queue/zipperQueue.js'
import cors from '@fastify/cors'
import fastifyFormbody from '@fastify/formbody'
import fastifySensible from '@fastify/sensible'
import { Buffer } from "node:buffer"
import startWorker from './lib/queue/zipperWorker.js'
import { Job } from 'bullmq'
import { finished } from 'node:stream/promises'
import { PassThrough } from 'node:stream'

const app = Fastify({ logger: true, requestTimeout: 0 })
app.register(fastifySensible)

const pubKey = readFileSync(
  process.env.NODE_ENV === 'development'
    ? '../_local_dev_keys/public.pem'
    : '/keys/public.pem'
)

app.register(cors, {
  origin: true, // Allow all origins
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['*'],
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

const tus = new TusServer({
  path: "/upload",
  datastore: provider.datastore,
  namingFunction: async (req) => {
    const name = await provider.namingFunction(req)
    // console.log("name:", name)
    return name
  },
  generateUrl(req, { proto, host, path, id }) {
    const encoded = Buffer.from(id, "utf8").toString("base64url");
    return `${proto}://${host}${path}/${encoded}`;
  },
  getFileIdFromRequest(req, lastPath) {
    return Buffer.from(lastPath, "base64url").toString("utf8");
  },
})

app.addContentTypeParser(
  "application/offset+octet-stream",
  (request, payload, done) => done(null)
);

const handleDownload = async (req, reply) => {
  const { tid, size, filesCount, name } = req.auth

  let hasBundle = null

  /** @type {Job} */
  const zipperJob = await zipperQueue.getJob(tid)
  if (zipperJob) {
    const state = await zipperJob.getState()
    if (["active", "delayed", "failed", "waiting"].includes(state)) {
      // Doesn't have bundle if job is in this state
      hasBundle = false

      console.warn(`The zipper job for ${tid} was in '${state}' state while downloading.`)
    }
  }

  // If hasBundle hasnt been set to false with the zipperJob check
  if (hasBundle === null) {
    // Maybe has a bundle
    hasBundle = await provider.hasBundle(tid)
  }

  console.log("has bundle ?", hasBundle)
  let result
  if (hasBundle) {
    // Returns either stream with fileType, or a download url
    result = await provider.prepareBundleSaved(tid, name)
  }
  else {
    if (!zipperJob) {
      throw new Error(`Transfer ${tid} does not have a bundle, and does not have a zipper job. Something is wrong! We can not get the file data if the job can't be found.`)
    }

    const { filesList } = zipperJob.data

    reply.header('Content-Type', "application/zip")
    reply.header('Content-Disposition', `attachment; filename="${name}"`)

    // reply.raw.write(`Content-Type: application/zip\r\nContent-Disposition: attachment; filename="${name}"\r\n\r\n`)

    const passThrough = new PassThrough()
    reply.send(passThrough)
    await provider.prepareZipBundleArchive(tid, filesList, passThrough)
    return
  }

  const { url, stream, fileType } = result
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

const handleUpload = (req, reply) => {
  // console.log("pre auth:", req.auth)
  // console.log(req.raw)
  tus.handle(req.raw, reply.raw)
}

const handleControlTransferStatus = async (req) => {
  const { transferId } = req.body
  const hasZipBundle = await provider.hasBundle(transferId)
  return { hasZipBundle }
}

const handleControlTransferDelete = async (req) => {
  const { transferId } = req.body
  await provider.delete(transferId)
  return { success: true }
}

const handleControlUploadComplete = async (req) => {
  const { transferId, filesList } = req.body

  // If there are more than one file, it should be zipped into the bundle
  // If there is only one file, the bundle IS that file already (to avoid zipping one file)
  if (filesList.length > 1) {
    console.log("Adding to zipperQueue:", transferId, filesList)
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
  url: '/upload',
  preHandler: needsScope('upload'),
  handler: (req, reply) => {
    handleUpload(req, reply)
  }
})

app.route({
  method: ['OPTIONS', 'HEAD', 'POST', 'PATCH'],
  url: '/upload/*',
  preHandler: needsScope('upload'),
  handler: (req, reply) => {
    handleUpload(req, reply)
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

process.on('uncaughtException', err => {
  console.error('[PROCESS LEVEL] Uncaught Exception:', err)
})

process.on('unhandledRejection', reason => {
  console.error('[PROCESS LEVEL] Unhandled Rejection:', reason)
})

startWorker()
await app.listen({ port: 3050, host: '0.0.0.0' })

