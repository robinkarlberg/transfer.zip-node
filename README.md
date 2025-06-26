# Transfer.zip-node

> [!WARNING] 
> This is Work in Progress, not working yet

Repo for [Transfer.zip](https://Transfer.zip) "node" server.

A node server handles file operations and storage. They can be distributed across the world to be close to users.

They can be configured for different types of storage: S3 compatible APIs and local disk.

## Setting Up

### Env

Copy the example configuration and env and edit them.

```
cd server
cp conf.json.example conf.json
cp .env.example .env
```

Edit `conf.json`

### Cryptographic keys

You need the public key from your [transfer.zip-web](https://github.com/robinkarlberg/transfer.zip-web) server.

Documentation how to do this is in that repo.

## Authentication

The Transfer.zip API server signs a JWT, which then is validated for each node.

The node server never contacts the main API server, the token is received from the browser.
