# Couchbase Service

*Couchbase simplified*

## Purpose

This purpose of this package is to provide a clean interface to the Node.js Couchbase SDK with built in support for reconnecting to Couchbase upon connection failure.

## Publishing a new version

Use the npm version command to increment the package version and push the code/tags.  The command can be run using `npm version <type>` where type is one of `major`, `minor`, `patch`.  The `postversion` npm script will execute after the version has been updated to push the code and tags to the origin server.
