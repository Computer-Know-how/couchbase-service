/**
 * DEPENDENCIES
 * ----------------------------------------------------------------------
 */

const { Cluster, ViewQuery, ViewResult, N1qlQuery } = require('couchbase');
const couchbase = require('couchbase')
const { operation } = require('retry');

/**
 * COUCHBASE SERVICE CLASS
 * ----------------------------------------------------------------------
 */

class CouchbaseService {
	constructor (bucketName, configOptions) {
		try {
			this.configOptions = configOptions;
			couchbase.connect(configOptions.cluster, configOptions.auth, (error ,cluster) => {
				this.cluster = cluster;
				this.bucket = this.cluster.bucket(bucketName);
				this.collection = this.bucket.defaultCollection();
				this.configOptions.operationTimeout = this.operationTimeout = configOptions.operationTimeout || 10000;
				this.collection.bucketName = this.bucketName = bucketName;
				this.atomicCounter = configOptions.atomicCounter;
				this.onReconnectCallback = configOptions.onReconnectCallback;
				this.binaryCollection = this.collection.binary();
				const checkConnection = () => {
					if (this.cluster._conns[bucketName]._connected === true){
						clearInterval(interval);
						this.cluster.ping(configOptions.onConnectCallback)
					}
				}
				const interval = setInterval(checkConnection, 100);
				setTimeout(() => {
					if (interval._destroyed === false) {
						console.error('Failed to connect to bucked: Timeout Error');
						console.trace();
						process.exit(1)
					}
				}, 8000);
				this.reconnectOptions = Object.assign({
					retries: 5,
					factor: 1.1,
					minTimeout: 1000,
					maxTimeout: 2000,
					randomize: false
				}, configOptions.reconnect);

				this.retryOptions = Object.assign({
					retries: 20,
					factor: 1.25,
					minTimeout: 200,
					maxTimeout: 2000,
					randomize: false
				}, configOptions.retry);
				return this;
			});
		} catch (e) {
			if (typeof configOptions.onConnectCallback === 'function') {
				configOptions.onConnectCallback(e);
			} else if (e) throw e;
		}
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
	getCallback(key, callback) { // callback: (error, value)
		this.bucketCall(
			'get',
			[key],
			(error, result) => {
				return (error) ? callback(error, null) : callback(null, result.value);
			}
		);
	}

	/**
	 * Retrieves a couchbase document from Couchbase & locks the document
	 * @function getAndLockCallback
	 * @param {string} key
	 * @param {string} lockTime
	 */
	getAndLockCallback(key, lockTime, callback) { // callback: (error, result)
		return this.bucketCall('getAndLock', [key, lockTime], callback);
	}

	/**
	 * gets documents from Couchbase standard bucket
	 * @function getMultiCallback
	 * @param {string} keys
	 */
	getMultiCallback(keys, callback) { // callback: (error, result)
		try {
			let errors = 0;
			const results = {};
			keys = this.removeDuplicates(keys);
			keys.map(key => {
				this.bucketCall('get', [key], (error, result) => {
					if (error) {
						errors++;
						results[key] = { error };
						if (Object.keys(results).length === keys.length) {
							return callback(errors || 0, results);
						}
					} else {
						results[key] = result;
						if (Object.keys(results).length === keys.length) {
							return callback(errors || 0, results);
						}
					}
				});
			});
		} catch (e) {
			return callback(e, null);
		}
	}

	/**
	 * Increment the atomic counter and get result
	 * @function counterCallback
	 */
	counterCallback(callback) { // callback: (error, result)
		return this.bucketCall('counter', [this.configOptions.atomicCounter, 1, { initial: 1 }], callback);
	}

	/**
	 * Inserts new message into Couchbase
	 * @function insertCallback
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	insertCallback(key, value, options, callback) { // callback: (error, result)
		if (options instanceof Function) {
			callback = options;
			options = {};
		}
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
		if (options instanceof Function) {
			callback = options;
			options = {};
		}
		if(isPopulatedObject(options) && 'cas' in options) {
			return this.bucketCall('replace', [key, value, options], callback);
		} else {
			return this.bucketCall('upsert', [key, value, options], callback);
		}
	}

	/**
	 * Inserts new message into Couchbase
	 * @function replaceCallback
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	replaceCallback(key, value, options, callback) { // callback: (error, result)
		try {
			if (options instanceof Function) {
				callback = options;
				options = {};
			}

			return this.bucketCall('replace', [key, value, options], callback);

		} catch (e) {
		}
	}

	/**
	 * Removes a document from Couchbase
	 * @function removeCallback
	 * @param {string} key
	 * @param {object} options
	 */
	removeCallback(key, options, callback) { // callback: (error, result)
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

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
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

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
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

		if(options.order && options.order === 'descending') {
			options.descending = true;
			delete options.order;
		} else if (options.order && options.order === 'ascending') {
			options.descending = false;
			delete options.order;
		}

		return this.bucket.viewQuery(ddoc, name, options, callback)
	}
	/**
	 * Run a Couchbase N1QL query
	 * @function n1qlQueryCallback
	 * @param {string} qry - Query string to run
	 */
	n1qlQueryCallback(qry, callback) { // callback: (error, result, meta)
		this.bucketCall('query', [qry], callback);
	}

	/**
	 * Inserts or updates a design document (view query)
	 * @function upsertDesignDocumentCallback
	 * @param {string} name - Name of the design document
	 * @param {object} views - Views to add
	 * @param {boolean} development - If true, install the design document in the dev documents
	 */
	upsertDesignDocumentCallback(name, views, development=false, callback) { // callback: (error)
		try {
			const manager = this.collection.manager(this.configOptions.auth.username, this.configOptions.auth.password);

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
				(error, result) => error ? reject(error) : resolve(result.value)
			);
		});
	}

	/**
	 * Retrieves a document from Couchbase & locks the document
	 * @function getAndLockPromise
	 * @param {string} key
	 * @param {string} lockTime
	 */
	getAndLockPromise(key, timeout) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'getAndLock',
				[key, timeout],
				(error, result) => error ? reject(error) : resolve(result)
			);
		});
	}

	/**
	 * Get the atomic counter from Couchbase
	 * @function getMultiPromise
	 * @param {string} keys
	 */
	getMultiPromise(keys) {
		keys = this.removeDuplicates(keys);
		return new Promise((resolve) => {
			this.getMultiCallback(keys, (error, result) => resolve({ error, result }));
		});
	}

	/**
	 * Checks to see if doc exists in Couchbase
	 * @function exists
	 * @param {string} keys
	 */
	exists(key) {
		return new Promise(async (resolve, reject) => {
			const exists = await this.collection.exists(key);
			return resolve(exists);
		});
	}

	/**
	 * Increment the atomic counter and get result
	 * @function counterPromise
	 */
	counterPromise() {
		return new Promise((resolve, reject) => {
			this.counterCallback((error, result) => {
				return error ? reject(error) : resolve(result);
			})
		});
	}

	/**
	 * Inserts new message into Couchbase
	 * @function insertPromise
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	insertPromise(key, value, options={}) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'insert',
				[key, value, options],
				(error, result) => error ? reject(error) : resolve(result)
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
	upsertPromise(key, value, options={}) {
		if(isPopulatedObject(options) && 'cas' in options) {
			return new Promise((resolve, reject) => {
				this.bucketCall(
					'replace',
					[key, value, options],
					(error, result) => error ? reject(error) : resolve(result)
				);
			});
		} else {
			return new Promise((resolve, reject) => {
				this.bucketCall(
					'upsert',
					[key, value, options],
					(error, result) => error ? reject(error) : resolve(result)
				);
			});
		}
	}

	/**
	 * Inserts new message into Couchbase
	 * @function upsertPromise
	 * @param {object} key
	 * @param {object} value
	 * @param {object} options
	 */
	replacePromise(key, value, options={}) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'replace',
				[key, value, options],
				(error, result) => error ? reject(error) : resolve(result)
			);
		});
	}

	/**
	 * Removes a document from Couchbase
	 * @function removePromise
	 * @param {string} key
	 * @param {object} options
	 */
	removePromise(key, options={}) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'remove',
				[key, options],
				(error, result) => error ? reject(error) : resolve(result)
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
	touchPromise(key, expiry, options={}) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'touch',
				[key, expiry, options],
				(error, result) => error ? reject(error) : resolve(result)
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
				(error, result) => error ? reject(error) : resolve(result)
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
		if(options.order && options.order === 'descending') {
			options.descending = true;
			delete options.order;
		} else if (options.order && options.order === 'ascending') {
			options.descending = false;
			delete options.order;
		}

		return new Promise( async (resolve, reject) => {
			try {
				var viewResult = await this.bucket.viewQuery(ddoc, name, options);
				return resolve(viewResult);
			} catch(e) {
				return reject(e);
			}
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
				[qry],
				(error, result) => error ? reject(error) : resolve(result)
			);
		});
	}

	/**
	 * Inserts/updates design documents.
	 * @function upsertDesignDocumentPromise
	 * @param {string} name
	 * @param {object} data
	 */
	upsertDesignDocumentPromise(name, views, development=true) {
		return new Promise((resolve, reject) => {
			const manager = this.collection.manager(this.configOptions.auth.username, this.configOptions.auth.password);

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
	 * Translates new error code to former code
	 * @function mapErrorCodes
	 * @param {object} error
	 */
	mapErrorCodes(error) {
		if(error instanceof couchbase.DocumentNotFoundError) {
			error.code = 13;
		} else if(error instanceof couchbase.DocumentExistsError){
			error.code = 12;
		} else if(error instanceof couchbase.TemporaryFailureError || error instanceof couchbase.DocumentLockedError){
			error.code = 11;
		} else if (error instanceof couchbase.TimeoutError){
			error.code = 23;
		}
		return error;
	}

	/**
	 * Couchbase bucket interaction abstraction
	 * @function bucketCall
	 * @param {string} method - Couchbase method (get, insert, etc.)
	 * @param {array} args - Array of arguments to pass to the Couchbase method call
	 * @returns {callback}
	 */
	bucketCall(method, args, callback) { // callback: (error, result)
		try {
			const op = operation(this.retryOptions);
			op.attempt(() => {
				if(!['counter', 'query'].includes(method)) {
					this.collection[method](...args, (error, result, meta) => {
						error = this.mapErrorCodes(error);
						const delay = error === 11 ? 500 : 0;
						setTimeout(() => {
							if (error && [11, 16, 23].includes(error.code)) {
								if (typeof this.onReconnectCallback === 'function') {
									this.onReconnectCallback(null, `Attempt Couchbase reconnect to ${this.bucketName} bucket. Error Code: ${error.code} Bucket Connected Status: ${this.bucket.connected}`);
								}
								if (!op.retry(error)) {
									return callback(error, result, meta);
								}
							} else {
								return callback(error, result, meta);
							}
						},delay);
					});
				} else if (method === 'counter') {
					return this.collection.binary().increment(...args , (error, result, meta) => {
						const delay = error === 11 ? 500 : 0;
						setTimeout(() => {
							if (error && [11, 16, 23].includes(error.code)) {
								if (typeof this.onReconnectCallback === 'function') {
									this.onReconnectCallback(null, `Attempt Couchbase reconnect to ${this.bucketName} bucket. Error Code: ${error.code} Bucket Connected Status: ${this.bucket.connected}`);
								}
								if (!op.retry(error)) {
									return callback(error, result, meta);
								}
							} else {
								return callback(error, result, meta);
							}
						},delay);
					});
				} else if (method === 'query'){
					return this.cluster.query(...args, (error, result, meta) => {
						const delay = error === 11 ? 500 : 0;
						setTimeout(() => {
							if (error && [11, 16, 23].includes(error.code)) {
								if (typeof this.onReconnectCallback === 'function') {
									this.onReconnectCallback(null, `Attempt Couchbase reconnect to ${this.bucketName} bucket. Error Code: ${error.code} Bucket Connected Status: ${this.bucket.connected}`);
								}
								if (!op.retry(error)) {
									return callback(error, result.rows, meta);
								}
							} else {
								return callback(error, result.rows, meta);
							}
						},delay);
					});
				} else {
					return this.bucket.viewQuery()(...args , (error, result, meta) => {
						const delay = error === 11 ? 500 : 0;
						setTimeout(() => {
							if (error && [11, 16, 23].includes(error.code)) {
								if (typeof this.onReconnectCallback === 'function') {
									this.onReconnectCallback(null, `Attempt Couchbase reconnect to ${this.bucketName} bucket. Error Code: ${error.code} Bucket Connected Status: ${this.bucket.connected}`);
								}
								if (!op.retry(error)) {
									return callback(error, result, meta);
								}
							} else {
								return callback(error, result, meta);
							}
						},delay);
					});
				}
			});
		} catch (e) {
			if (/shutdown bucket/.exec(e.message)) {
				this.reconnectBucket(0, err => {
					return err
						? callback(err, null, null)
						: this.bucketCall(method, args, callback);
				});
			} else {
				return callback(e, null, null);
			}
		}
	}

	/**
	 * Reconnects service to the Couchbase instance after Couchbase has been restored
	 * @function reconnectBucket
	 */
	reconnectBucket(errorCode, callback) { // callback: (error)
		try {
			const op = operation(this.reconnectOptions);

			op.attempt(() => {
				const delay = errorCode === 11 ? 500 : 0;

				if (typeof this.onReconnectCallback === 'function') {
					this.onReconnectCallback(null, `Attempt Couchbase reconnect to ${this.bucketName} bucket. Error Code: ${errorCode} Bucket Connected Status: ${this.bucket.connected}`);
				}
				setTimeout(() => {
					if (this.bucket.connected) {
						this.bucket.disconnect();
					}

					this.bucket = this.cluster.bucket(this.bucketName, err => {
						if (err) {
							if (op.retry(err)) {
								return; // retry until done
							} else {
								if (typeof this.onReconnectCallback === 'function') {
									this.onReconnectCallback(`Couchbase reconnection test failed. Attempted to reconnect to ${this.bucketName} bucket. Error: ${err}`, null);
								}
								return callback(new Error('connecting to Couchbase failed, aborting operation'));
							}
						} else {
							if (typeof this.onReconnectCallback === 'function') {
								this.onReconnectCallback(null, `Reconnected to Couchbase ${this.bucketName} bucket. Bucket Connected Status: ${this.bucket.connected}.`);
							}
							this.bucket.operationTimeout = this.operationTimeout;
							this.bucket.bucketName = this.bucketName;
							this.bucket.atomicCounter = this.atomicCounter;
							return callback(null);
						}
					});
				}, delay);
			});
		} catch (e) {
			return callback(e);
		}
	}

	removeDuplicates(arr) {
		return Array.from(new Set(arr));
	}
}

function isPopulatedObject(value) {
	return (value && value instanceof Object && !Array.isArray(value) && Object.keys(value).length) ? true : false;
}

/**
 * EXPORTS
 * ----------------------------------------------------------------------
 */

module.exports = CouchbaseService;
