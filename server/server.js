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
  const { tid, size, filesCount, firstFile } = req.auth

  let result
  const hasBundle = await provider.hasBundle(tid)
  if (hasBundle) {
    // Returns either stream with fileInfo, or an download url
    result = await provider.prepareBundleSaved(tid, firstFile?.name ?? "transfer.zip")
  }
  else {
    const zipperJob = await zipperQueue.getJob(tid)
    if (!zipperJob) {
      throw new Error(`Transfer ${tid} does not have a bundle, and does not have a zipper job. Something is wrong!`)
    }

    const { filesList } = zipperJob.data

    const { stream, done /* "done" is just for logging */ } = await provider.prepareZipBundleStream(tid, filesList)
    result = {
      stream,
      fileInfo: {
        type: "application/zip",
        name: "transfer.zip",
      }
    }
  }

  const { url, stream, fileInfo } = result
  if (url) {
    reply.redirect(url)
  }
  else {
    const { type, name } = fileInfo
    reply.header('Content-Type', type)
    reply.header('Content-Disposition', `attachment; filename="${name}"`)

    reply.send(stream)
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

const handleControlUploadComplete = async (req) => {
  const { transferId, filesList } = req.body

  // If there are more than one file, it should be zipped into the bundle
  // If there is only one file, the bundle IS that file already (to avoid zipping one file)
  if (filesList.length > 1) {
    console.log("Adding to zipperQueue:", transferId, filesList)
    await zipperQueue.add(`${transferId}-zipper`, { filesList }, { jobId: transferId })
  }

  return { success: true }
}

app.register(async function (app) {
  await app.register(fastifyFormbody)

  app.post('/download', { preHandler: needsScope('download', true) }, async (req, reply) => {
    return await handleDownload(req, reply)
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

app.post('/control/uploadComplete', { preHandler: needsScope('control') }, async (req, reply) => {
  return await handleControlUploadComplete(req, reply)
})

app.get('/healthz', () => ({ success: true }))

startWorker()
await app.listen({ port: 3050, host: '0.0.0.0' })