# Transfer.zip-node

Repo for [Transfer.zip](https://Transfer.zip) "node" server.

A node server handles file operations and storage. They can be distributed across the world to be close to users.

They can be configured for different types of storage: S3 compatible APIs and local disk.

## Setting Up

### Environment

Copy the example configuration and env and edit them.

```
./createenv.sh
```

Edit `.env server/.env server/conf.json`.

conf.json format: TODO

### Cryptographic keys & Authentication

The Transfer.zip API server signs a JWT, which then is validated for each node, making sure all requests are authenticated and valid.
The node server never contacts the main API server, the signed JWTs are always received via the browser or the main API server itself (when deleting transfers for example).

You need the public key from your [transfer.zip-web](https://github.com/robinkarlberg/transfer.zip-web) server.
Documentation how to do this is in that repo.

Put the public key in `./keys/public.pem` in the top of this repo, that way the node server can find it.

## Running the server

The easiest way to run the server is to use the Caddy server that's shipped with the repo
```
./deploy-caddy
```

If you want to configure your own 
```
docker compose up -d
```
