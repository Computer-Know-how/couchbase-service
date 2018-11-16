/**
 * DEPENDENCIES
 * ----------------------------------------------------------------------
 */

const couchbase = require('couchbase');
const retry = require('retry');

const { ViewQuery, N1qlQuery } = couchbase;

/**
 * COUCHBASE SERVICE CLASS
 * ----------------------------------------------------------------------
 */

class CouchbaseService {
	constructor (bucketName, options) {
		this.cluster = new couchbase.Cluster(options.cluster);

		// If auth options are present, authenticate the cluster
		if (options.auth) {
			this.cluster.authenticate(options.auth.username, options.auth.password);
		}

		this.options = options;
		this.bucketName = bucketName;

		// Open the Couchbase bucket
		this.bucket = this.cluster.openBucket(bucketName, error => {
			if (typeof options.onConnectCallback === 'function') {
				options.onConnectCallback(error);
			} else if (error) throw error;

			// Set bucket settings
			this.bucket.operationTimeout = this.operationTimeout = options.operationTimeout || 10000;
			this.bucket.bucketName = bucketName;
			this.bucket.atomicCounter = options.atomicCounter;
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
		return this.bucketCall('counter', [this.bucket.atomicCounter], callback);
	}

	/**
	 * Inserts new message into Couchbase
	 * @function insertCallback
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	insertCallback(key, value, options, callback) { // callback: (error, result)
		return this.bucketCall('insert', [key, value, options], callback);
	}

	/**
	 * Inserts new message into Couchbase
	 * @function upsertCallback
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	upsertCallback(key, value, options, callback) { // callback: (error, result)
		return this.bucketCall('upsert', [key, value, options], callback);
	}

	/**
	 * Removes a document from Couchbase
	 * @function removeCallback
	 * @param {string} key
	 * @param {object} options
	 */
	removeCallback(key, options, callback) { // callback: (error, result)
		return this.bucketCall('remove', [key, options], callback);
	}

	/**
	 * Removes a document from Couchbase
	 * @function touchCallback
	 * @param {string} key
	 * @param {object} expiry
	 * @param {object} options
	 */
	touchCallback(key, expiry, options, callback) { // callback: (error, result)
		return this.bucketCall('touch', [key, expiry, options], callback);
	}

	/**
	 * Removes a document from Couchbase
	 * @function unlockCallback
	 * @param {string} key
	 * @param {object} cas
	 */
	unlockCallback(key, cas, callback) { // callback: (error, result)
		return this.bucketCall('unlock', [key, cas], callback);
	}

	/**
	 * Run a Couchbase query
	 * @function viewQueryCallback
	 * @param {string} ddoc - Design document name
	 * @param {string} name - View name
	 * @param {object} options - View query options
	 */
	viewQueryCallback(ddoc, name, options, callback) { // callback: (error, result, meta)
		return this.bucketCall('query', [this.prepareViewQuery(ddoc, name, options)], callback);
	}

	/**
	 * Run a Couchbase N1QL query
	 * @function n1qlQueryCallback
	 * @param {string} qry - Query string to run
	 */
	n1qlQueryCallback(qry, callback) { // callback: (error, result, meta)
		return this.bucketCall('query', [N1qlQuery.fromString(qry)], callback);
	}

	/**
	 * Inserts or updates a design document (view query)
	 * @function upsertDesignDocumentCallback
	 * @param {string} name
	 * @param {object} data
	 */
	upsertDesignDocumentCallback(name, views, development=false, callback) { // callback: (error)
		try {
			const manager = this.bucket.manager(this.options.auth.username, this.options.auth.password);

			manager.upsertDesignDocument(development ? `dev_${name}` : name, { views }, callback);
		} catch(e) {
			return callback(e);
		}
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
				(error, result) => error ? reject(error) : reject(result)
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
				(error, result) => error ? reject(error) : reject(result)
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
				(error, result) => error ? reject(error) : reject(result)
			);
		});
	}

	/**
	 * Increment the atomic counter and get result
	 * @function getCounterPromise
	 */
	getCounterPromise() {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'counter',
				[this.bucket.atomicCounter],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
	}

	/**
	 * Inserts new message into Couchbase
	 * @function insertPromise
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	insertPromise(key, value, options) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'insert',
				[key, value, options],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
	}

	/**
	 * Inserts new message into Couchbase
	 * @function upsertPromise
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	upsertPromise(key, value, options) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'upsert',
				[key, value, options],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
	}

	/**
	 * Removes a document from Couchbase
	 * @function removePromise
	 * @param {string} key
	 * @param {object} options
	 */
	removePromise(key, options) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'remove',
				[key, options],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
	}

	/**
	 * Removes a document from Couchbase
	 * @function touchPromise
	 * @param {string} key
	 * @param {object} expiry
	 * @param {object} options
	 */
	touchPromise(key, expiry, options) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'touch',
				[key, expiry, options],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
	}

	/**
	 * Removes a document from Couchbase
	 * @function unlockPromise
	 * @param {string} key
	 * @param {object} cas
	 */
	unlockPromise(key, cas) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'unlock',
				[key, cas],
				(error, result) => error ? reject(error) : reject(result),
			);
		});
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
				[N1qlQuery.fromString(qry)],
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
	upsertDesignDocumentPromise(bucket, name, views, development=true) {
		return new Promise((resolve, reject) => {
			const manager = this.bucket.manager(this.options.auth.username, this.options.auth.password);

			manager.upsertDesignDocument(
				development ? `dev_${name}` : name,
				{ views },
				error => error ? reject(error) : resolve(null),
			);
		});
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
					this.reconnectBucket(error.code, err => {
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
				this.reconnectBucket(0, err => {
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

					this.bucket = this.cluster.openBucket(this.bucketName, err => {
						if (err) {
							if (operation.retry(err)) {
								return; // retry until done
							} else {
								return callback(new Error('connecting to Couchbase failed, aborting operation'));
							}
						} else {
							this.bucket.operationTimeout = this.operationTimeout;
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
		const query = couchbase.ViewQuery.from(ddoc, name);

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
					};

					query.stale(modes[opt]);
					break;
				}

				case 'order': {
					const modes = {
						ascending: ViewQuery.Order.ASCENDING,
						descending: ViewQuery.Order.DESCENDING,
					};

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
