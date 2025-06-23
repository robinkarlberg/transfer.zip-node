// storage-node/server.js
import Fastify from 'fastify';
import fastifyJwt from '@fastify/jwt';
import { readFileSync } from 'node:fs';
import { Server as TusServer } from '@tus/server';
import { FileStore } from '@tus/file-store';

const app = Fastify({ logger: true, requestTimeout: 0 });

const pubKey = readFileSync(process.env.NODE_ENV == "development" ? '../_local_dev_keys/public.pem' : '/keys/public.pem')

// JWT verification plugin (public key, RS256 only)
app.register(fastifyJwt, {
  secret: { public: pubKey },
  sign: {},                           // not used here
  verify: { algorithms: ['RS256'], audience: 'transfer.zip/upload' },
});

// Tus server instance (no global maxSize, we’ll apply it per-request)
// const tus = new TusServer({
//   path: '/files',
//   datastore: new FileStore({ directory: './uploads' }),
// });

// Fastify preHandler that
//     • verifies the token
//     • rejects if Upload-Length > token.maxSize
const canUpload = async (req, _reply) => {
  try {
    const { maxSize } = await req.jwtVerify();          // adds payload to req.user
    const lengthHeader = req.headers['upload-length'];

    if (lengthHeader && +lengthHeader > +maxSize) {
      throw req.server.httpErrors.payloadTooLarge(
        `Upload exceeds plan limit (${lengthHeader} B > ${maxSize} B)`
      );
    }
  } catch (err) {
    throw req.server.httpErrors.unauthorized(err.message);
  }
};

// Tell Fastify not to parse bodies for tus content types
const RAW = (_, payload, done) => done(null);
['application/offset+octet-stream', 'application/tus-resumable'].forEach(ct =>
  app.addContentTypeParser(ct, RAW)
);

// Wire everything up
app.all('/files', { preHandler: canUpload }, (req, res) => tus.handle(req.raw, res.raw));
app.all('/files/*', { preHandler: canUpload }, (req, res) => tus.handle(req.raw, res.raw));
app.get('/healthz', () => ({ ok: true }));

await app.listen({ port: 3050, host: '0.0.0.0' });
