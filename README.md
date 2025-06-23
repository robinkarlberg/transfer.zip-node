# Transfer.zip-node

Repo for [Transfer.zip](https://Transfer.zip) "node" server.

A node server handles file operations and storage. They can be distributed across the world to be close to users.

They can be configured for different types of storage: S3 compatible api's and local disk.

## Authentication

The Transfer.zip API server signs a JWT, which then is validated for each node.

