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

			if (typeof onConnectCallback === 'function') {
				return onConnectCallback();
			}
		});

		this.reconnectOptions = Object.assign({
			retries: 5,
			factor: 1.1,
			minTimeout: 1000,
			maxTimeout: 2000,
			randomize: false
		}, options.reconnect);

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
	 * @function viewQueryCallback
	 * @param {string} ddoc - Design document name
	 * @param {string} name - View name
	 * @param {object} options - View query options
	 */
	viewQueryCallback(ddoc, name, options, callback) { // callback: (error, result, meta)
		return this.bucket.query(this.prepareViewQuery(ddoc, name, options), callback);
	}

	/**
	 * Run a Couchbase N1QL query
	 * @function n1qlQueryCallback
	 * @param {string} qry - Query string to run
	 */
	n1qlQueryCallback(qry, callback) { // callback: (error, result, meta)
		return this.bucketCall('query', [couchbase.N1qlQuery.fromString(qry)], callback);
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
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'get',
				[key],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
	}

	/**
	 * Retrieves a document from Couchbase & locks the document
	 * @function getAndLockPromise
	 * @param {string} key
	 * @param {string} lockTime
	 */
	getAndLockPromise(key, lockTime) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'getAndLock',
				[key, { lockTime }],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
	}

	/**
	 * Get the atomic counter from Couchbase
	 * @function getMultiPromise
	 * @param {string} keys
	 */
	getMultiPromise(keys) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'getMutli',
				[keys],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
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
	 * Run a Couchbase view query
	 * @function viewQueryPromise
	 * @param {string} ddoc - Design document name
	 * @param {string} name - View name
	 * @param {object} options - View query options
	 */
	viewQueryPromise(ddoc, name, options) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'query',
				[this.prepareViewQuery(ddoc, name, options)],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
	}

	/**
	 * Run a Couchbase N1QL query
	 * @function n1qlQueryPromise
	 * @param {string} qry - Query string to run
	 */
	n1qlQueryPromise(qry) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'query',
				[couchbase.N1qlQuery.fromString(qry)],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
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

	prepareViewQuery(ddoc, name, options) {
		// Create the base view query
		const query = ViewQuery.from(ddoc, name);

		// Add view query options
		// Available options are:
		// - custom
		// - group
		// - group_level
		// - include_docs
		// - key
		// - keys
		// - limit
		// - order
		// - reduce
		// - skip
		// - stale

		for (const option of options) {
			const opt = options[option];

			switch (option) {
				case 'stale': {
					const modes = {
						before: ViewQuery.Update.BEFORE,
						none: ViewQuery.Update.NONE,
						after: ViewQuery.Update.AFTER,
					}

					query.stale(modes[opt]);
					break;
				}

				case 'order': {
					const modes = {
						ascending: ViewQuery.Order.ASCENDING,
						descending: ViewQuery.Order.DESCENDING,
					}

					query.order(modes[opt]);
					break;
				}

				case 'id_range':
					query[option](opt.start, opt.end);
					break;

				case 'range':
					query[option](opt.start, opt.end, opt.inclusive_end);
					break;

				default:
					query[option](opt);
			}
		}

		return query;
	}
}

/**
 * EXPORTS
 * ----------------------------------------------------------------------
 */

module.exports = CouchbaseService;
