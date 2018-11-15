/**
 * DEPENDENCIES
 * ----------------------------------------------------------------------
 */

const couchbase = require('couchbase');
const retry = require('retry');

/**
 * COUCHBASE SERVICE CLASS
 * ----------------------------------------------------------------------
 */

class CouchbaseService {
	constructor (bucketName, options, onConnectCallback) {
		const cluster = new couchbase.Cluster(options.cluster);

		// If auth options are present, authenticate the cluster
		if (options.auth) {
			cluster.authenticate(options.auth.username, options.auth.password);
		}

		this.bucketName = bucketName;
		this.bucket = cluster.openBucket(bucketName, error => {
			if (error) throw error;

			this.bucket.operationTimeout = 10000;
			this.bucket.bucketName = bucketName;
			this.bucket.atomicCounter = options.atomicCounter;

			return onConnectCallback();
		});

		this.reconnectOptions = Object.assign({
			retries: 5,
			factor: 1.1,
			minTimeout: 1000,
			maxTimeout: 2000,
			randomize: false
		}, options.reconnect || {});

		return this;
	}

	/**
	 * CALLBACK METHODS
	 * ----------------------------------------------------------------------
	 */

	/**
	 * Gets a document from Couchbase standard bucket
	 * @function getCallback
	 * @param {object} key
	 */
	getCallback(key, callback) { // callback: (error, result)
		return this.bucketCall('get', [key], callback);
	}

	/**
	 * Retrieves a couchbase document from Couchbase & locks the document
	 * @function getAndLockCallback
	 * @param {string} key
	 * @param {string} lockTime
	 */
	getAndLockCallback(key, lockTime, callback) { // callback: (error, result)
		return this.bucketCall('getAndLock', [key, { lockTime }], callback);
	}

	/**
	 * Get the atomic counter from Couchbase
	 * @function getMultiCallback
	 * @param {string} keys
	 */
	getMultiCallback(keys, callback) { // callback: (error, result)
		return this.bucketCall('getMulti', [keys], callback);
	}

	/**
	 * Increment the atomic counter and get result
	 * @function getCounterCallback
	 */
	getCounterCallback(callback) { // callback: (error, result)
	}

	/**
	 * Inserts new message into Couchbase
	 * @function insertCallback
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	insertCallback(key, value, options, callback) { // callback: (error, result)
	}

	/**
	 * Inserts new message into Couchbase
	 * @function upsertCallback
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	upsertCallback(key, value, options, callback) { // callback: (error, result)
	}

	/**
	 * Removes a document from Couchbase
	 * @function removeCallback
	 * @param {string} key
	 * @param {object} options
	 */
	removeCallback(key, options, callback) { // callback: (error, result)
	}

	/**
	 * Removes a document from Couchbase
	 * @function touchCallback
	 * @param {string} key
	 * @param {object} expiry
	 * @param {object} options
	 */
	touchCallback(key, expiry, options, callback) { // callback: (error, result)
	}

	/**
	 * Removes a document from Couchbase
	 * @function unlockCallback
	 * @param {string} key
	 * @param {object} cas
	 */
	unlockCallback(key, cas, callback) { // callback: (error, result)
	}

	/**
	 * Run a Couchbase query
	 * @function queryCallback
	 * @param {string} qry
	 */
	queryCallback(qry, callback) { //callback: (error, result, meta)
	}

	/**
	 * Inserts or updates a design document (view query)
	 * @function upsertDesignDocumentCallback
	 * @param {string} name
	 * @param {object} data
	 */
	upsertDesignDocumentCallback(name, data, callback) {
	}

	/**
	 * PROMISE METHODS
	 * ----------------------------------------------------------------------
	 */

	/**
	 * Gets a document from Couchbase standard bucket
	 * @function getPromise
	 * @param {object} key
	 */
	getPromise(key) {
	}

	/**
	 * Retrieves a document from Couchbase & locks the document
	 * @function getAndLockPromise
	 * @param {string} key
	 * @param {string} lockTime
	 */
	getAndLockPromise(key, lockTime) {

	}

	/**
	 * Get the atomic counter from Couchbase
	 * @function getMultiPromise
	 * @param {string} keys
	 */
	getMultiPromise(keys) {
	}

	/**
	 * Increment the atomic counter and get result
	 * @function getCounterPromise
	 */
	getCounterPromise() {
	}

	/**
	 * Inserts new message into Couchbase
	 * @function insertPromise
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	insertPromise(key, value, options) {
	}

	/**
	 * Inserts new message into Couchbase
	 * @function upsertPromise
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	upsertPromise(key, value, options) {
	}

	/**
	 * Removes a document from Couchbase
	 * @function removePromise
	 * @param {string} key
	 * @param {object} options
	 */
	removePromise(key, options) {
	}

	/**
	 * Removes a document from Couchbase
	 * @function touchPromise
	 * @param {string} key
	 * @param {object} expiry
	 * @param {object} options
	 */
	touchPromise(key, expiry, options) {
	}

	/**
	 * Removes a document from Couchbase
	 * @function unlockPromise
	 * @param {string} key
	 * @param {object} cas
	 */
	unlockPromise(key, cas) {
	}



	/**
	 * Runs a Couchbase query based on submitted query parameters
	 * @function queryPromise
	 * @param {string} qry
	 */
	queryPromise(qry) { //callback: (error, result, meta)
	}

	/**
	 * Inserts/updates design documents.
	 * @function upsertDesignDocumentPromise
	 * @param {string} name
	 * @param {object} data
	 */
	upsertDesignDocumentPromise(name, data) {
	}

	/**
	 * PRIVATE METHODS
	 * ----------------------------------------------------------------------
	 */

	/**
	 * Couchbase bucket interaction abstraction
	 * @function bucketCall
	 * @param {string} method - Couchbase method (get, insert, etc.)
	 * @param {array} args - Array of arguments to pass to the Couchbase method call
	 * @returns {callback}
	 */
	bucketCall(method, args, callback) { // callback: (error, result)
		try {
			this.bucket[method](...args, (error, result) => {
				if (error && [11, 16, 23].includes(error.code)) {
					this.reconnectBucket(context, error.code, err => {
						return err
							? callback(err, null)
							: this.bucketCall(method, args, callback);
					});
				} else {
					return callback(error, result);
				}
			});
		} catch (e) {
			if (/shutdown bucket/.exec(e.message)) {
				this.reconnectBucket(context, 0, err => {
					return err
						? callback(err, null)
						: this.bucketCall(method, args, callback);
				});
			} else {
				return callback(e, null);
			}
		}
	}

	/**
	 * Reconnects service to the Couchbase instance after Couchbase has been restored
	 * @function reconnectBucket
	 */
	reconnectBucket(errorCode, callback) { // callback: (error)
		try {
			const operation = retry.operation(this.reconnectOptions);

			operation.attempt(() => {
				const delay = errorCode === 11 ? 500 : 0;

				setTimeout(() => {
					if (this.bucket.connected) {
						this.bucket.disconnect();
					}

					this.bucket = cluster.openBucket(this.bucketName, err => {
						if (err) {
							if (operation.retry(err)) {
								return; // retry until done
							} else {
								return callback(new Error('connecting to Couchbase failed, aborting operation'));
							}
						} else {
							this.bucket.operationTimeout = operationTimeout;
							return callback(null);
						}
					});
				}, delay);
			});
		} catch (e) {
			return callback(e);
		}
	}
}

/**
 * EXPORTS
 * ----------------------------------------------------------------------
 */

module.exports = {
	CouchbaseService,
	ViewQuery: couchbase.ViewQuery,
	N1qlQuery: couchbase.N1qlQuery
};
