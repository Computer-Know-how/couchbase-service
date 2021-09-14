/*
	DEPENDENCIES -------------------------------------------------------------------------------------------------------------
*/

const couchbase = require('couchbase');

const { operation } = require('retry');


/*
	COUCHBASESERVICE -------------------------------------------------------------------------------------------------------------
*/

/**
	* COUCHBASE SERVICE CLASS
	* @class CouchbaseService
*/
class CouchbaseService {
	constructor (bucketName, configOptions) {
		try {
			this.configOptions = configOptions;
			couchbase.connect(configOptions.cluster, configOptions.auth, (error ,cluster) => {
				this.cluster = cluster;
				if (this.cluster) {
					this.bucket = this.cluster.bucket(bucketName);
				} else {
					return configOptions.onConnectCallback(new Error(`No bucket named ${bucketName} found, could not connect to Couchbase`));
				}

				this.collection = this.bucket.defaultCollection();
				this.configOptions.operationTimeout = this.operationTimeout = configOptions.operationTimeout || 10000;
				this.collection.bucketName = this.bucketName = bucketName;
				this.atomicCounter = configOptions.atomicCounter;
				this.onReconnectCallback = configOptions.onReconnectCallback;
				this.binaryCollection = this.collection.binary();

				let x = 0;
				const interval = setInterval( () => {
					try {
						if (x++ >= 16) {
							clearInterval(interval);
							if (this.cluster._conns[bucketName]._connected === false) {
								return configOptions.onConnectCallback(new Error(`Could not connect to Couchbase bucket ${bucketName}. Connection timed out.`));
							} else {
								return configOptions.onConnectCallback(null);
							}
						} else {
							if(this.cluster._conns[bucketName]._connected === true) {
								clearInterval(interval);
								return configOptions.onConnectCallback(null);
							}
						}
					} catch (e) {
						clearInterval(interval);
						return configOptions.onConnectCallback(new Error(`Could not connect to Couchbase bucket ${bucketName}. Error: ${e.message}`));
					}
				}, 500);

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
		CALLBACK METHODS -------------------------------------------------------------------------------------------------
	*/

	/**
		* @function getCallback - gets document by key from bucket
		* @param {String} key
		* @param {Function} callback: (error, value)
	*/
	getCallback(key, callback) {
		this.bucketCall(
			'get',
			[key],
			(error, result) => (error) ? callback(error, null) : callback(null, valueFromResult(result))
		);
	}

	/**
		* @function getAndLockCallback - gets & locks a document by key from bucket
		* @param {String} key
		* @param {String} lockTime
		* @param {Function} callback: (error, result)
	*/
	getAndLockCallback(key, lockTime, callback) {
		return this.bucketCall(
			'getAndLock',
			[key, lockTime],
			callback
		);
	}

	/**
		* @function getMultiCallback - gets documents from bucket
		* @param {Array} keys - Array of document keys (Strings)
		* @param {Function} callback: (error, result)
	*/
	getMultiCallback(keys, callback) {
		try {
			let errors = 0, results = {};
			keys = this.removeDuplicates(keys);

			keys.map(key => {
				this.bucketCall('get', [key], (error, result) => {
					if (error) {
						errors++;
						results[key] = { error };
						if (Object.keys(results).length === keys.length) {
							return callback(errors, results);
						}
					} else {
						results[key] = result;
						if (Object.keys(results).length === keys.length) {
							return callback(errors, results);
						}
					}
				});
			});
		} catch(e) {
			return callback(e, null);
		}
	}

	/**
		* @function counterCallback - increments atomic counter, returns post-incremented value
		* @param {Function} callback: (error, result)
	*/
	counterCallback(callback) {
		return this.bucketCall(
			'counter',
			[this.configOptions.atomicCounter, 1, { initial: 1 }],
			callback
		);
	}

	/**
		* @function insertCallback - inserts new value into bucket
		* @param {String} key
		* @param {Object|String} value
		* @param {Object} options
		* @param {Function} callback: (error, result)
	*/
	insertCallback(key, value, options, callback) {
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

		return this.bucketCall(
			'insert',
			[key, value, options],
			callback
		);
	}

	/**
		* @function upsertCallback - upserts value over existing document in bucket
		* @param {String} key
		* @param {Object|String} value
		* @param {Object} options
		* @param {Function} callback: (error, result)
	*/
	upsertCallback(key, value, options, callback) {
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

		return this.bucketCall(
			isObjectWithProperty(options, 'cas') ? 'replace' : 'upsert',
			[key, value, options],
			callback
		);
	}

	/**
		* @function replaceCallback - replaces existing message in bucket
		* @param {String} key
		* @param {Object|String} value
		* @param {Object} options
		* @param {Function} callback: (error, result)
	*/
	replaceCallback(key, value, options, callback) {
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

		return this.bucketCall(
			'replace',
			[key, value, options],
			callback
		);
	}

	/**
		* @function removeCallback - removes a document from bucket
		* @param {String} key
		* @param {Object} options
		* @param {Function} callback: (error, result)
	*/
	removeCallback(key, options, callback) {
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

		return this.bucketCall(
			'remove',
			[key, options],
			callback
		);
	}

	/**
		* @function touchCallback - touches a document in bucket
		* @param {String} key
		* @param {Object} expiry
		* @param {Object} options
		* @param {Function} callback: (error, result)
	 */
	touchCallback(key, expiry, options, callback) {
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

		return this.bucketCall(
			'touch',
			[key, expiry, options],
			callback
		);
	}

	/**
		* @function unlockCallback - unlocks a document in bucket
		* @param {String} key
		* @param {Object} cas
		* @param {Function} callback: (error, result)
	*/
	unlockCallback(key, cas, callback) {
		return this.bucketCall(
			'unlock',
			[key, cas],
			callback
		);
	}

	/**
		* @function viewQueryCallback - runs a view query request on a bucket
		* @param {String} ddoc - design document name
		* @param {String} name - view name
		* @param {Object} options
		* @param {Function} callback: (error, result, meta)
	*/
	viewQueryCallback(ddoc, name, options, callback) {
		if (options instanceof Function) {
			callback = options;
			options = {};
		}

		if(options.order) {
			if(options.order === 'descending') {
				options.descending = true;
			} else if(options.order === 'ascending') {
				options.descending = false;
			}
			delete options.order;
		}

		return this.bucket.viewQuery(
			ddoc,
			name,
			options,
			callback
		);
	}

	/**
		* @function n1qlQueryCallback - run a Couchbase N1QL query
		* @param {String} qry - n1ql query string
		* @param {Function} callback: (error, result, meta)
	*/
	n1qlQueryCallback(qry, callback) {
		this.bucketCall(
			'query',
			[qry],
			callback
		);
	}

	/**
		* @function upsertDesignDocumentCallback - inserts or updates a design document (view query)
		* @param {String} name - design document name
		* @param {Object} views - Views to add
		* @param {Boolean} development - if true, install design document in dev documents
		* @param {Function} callback: (error)
	*/
	upsertDesignDocumentCallback(name, views, development=false, callback) {
		try {
			const manager = this.collection.manager(this.configOptions.auth.username, this.configOptions.auth.password);

			manager.upsertDesignDocument(
				development ? `dev_${name}` : name,
				{ views },
				callback
			);
		} catch(e) {
			return callback(e);
		}
	}

	/**
		PROMISIFIED METHODS -----------------------------------------------------------------------------------------------------------------------
	*/

	/**
		* @function getPromise - gets a document by key from bucket
		* @param {String} key
		* @returns {Promise}
	*/
	getPromise(key) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				'get',
				[key],
				(error, result) => error ? reject(error) : resolve(valueFromResult(result))
			);
		});
	}

	/**
		* @function getAndLockPromise - gets & locks a document by key from bucket
		* @param {String} key
		* @param {Number} timeout
		* @returns {Promise}
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
		* @function getMultiPromise - gets documents from bucket
		* @param {Array} keys
		* @returns {Promise}
	*/
	getMultiPromise(keys) {
		keys = this.removeDuplicates(keys);
		return new Promise((resolve) => {
			this.getMultiCallback(
				keys,
				(error, result) => resolve({ error, result })
			);
		});
	}

	/**
		* @function exists - checks if doc exists in bucket
		* @param {String} key
		* @returns {Promise}
	*/
	exists(key) {
		return new Promise(async (resolve) => {
			const exists = await this.collection.exists(key);
			return resolve(exists);
		});
	}

	/**
		* @function counterPromise - increments atomic counter, returns post-incremented value
		* @returns {Promise}
	*/
	counterPromise() {
		return new Promise((resolve, reject) => {
			this.counterCallback((error, result) => error ? reject(error) : resolve(result));
		});
	}

	/**
		* @function insertPromise - inserts new document into bucket
		* @param {String} key
		* @param {Object|String} value
		* @param {Object} options
		* @returns {Promise}
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
		* @function upsertPromise - upserts value over existing document in bucket
		* @param {String} key
		* @param {Object|String} value
		* @param {Object} options
		* @returns {Promise}
	*/
	upsertPromise(key, value, options={}) {
		return new Promise((resolve, reject) => {
			this.bucketCall(
				isObjectWithProperty(options, 'cas') ? 'replace' : 'upsert',
				[key, value, options],
				(error, result) => error ? reject(error) : resolve(result)
			);
		});
	}

	/**
		* @function upsertPromise - upserts value over existing document in bucket
		* @param {String} key
		* @param {Object|String} value
		* @param {Object} options
		* @returns {Promise}
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
		* @function removePromise - remove a document from bucket
		* @param {String} key
		* @param {Object} options
		* @returns {Promise}
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
		* @function touchPromise - touches a document in bucket
		* @param {String} key
		* @param {Object} expiry
		* @param {Object} options
		* @returns {Promise}
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
		* @function unlockPromise - unlocks document in bucket
		* @param {String} key
		* @param {Object} cas
		* @returns {Promise}
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
		* @function viewQueryPromise - runs a view query request on a bucket
		* @param {String} ddoc - design document name
		* @param {String} name - view name
		* @param {Object} options
		* @returns {Promise}
	*/
	viewQueryPromise(ddoc, name, options) {
		if(options.order) {
			if(options.order === 'descending') {
				options.descending = true;
			} else if(options.order === 'ascending') {
				options.descending = false;
			}
			delete options.order;
		}

		return new Promise(async(resolve, reject) => {
			try {
				var viewResult = await this.bucket.viewQuery(ddoc, name, options);
				return resolve(viewResult);
			} catch(e) {
				return reject(e);
			}
		});
	}

	/**
		* @function n1qlQueryCallback - run a Couchbase N1QL query
		* @param {String} qry - n1ql query string
		* @returns {Promise}
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
		* @function upsertDesignDocumentPromise - inserts or updates a design document (view query)
		* @param {String} name - design document name
		* @param {Object} views - Views to add
		* @param {Boolean} development - if true, install design document in dev documents
		* @returns {Promise}
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
		META FUNCTIONS ---------------------------------------------------------------------------------------------------------------------------------------
	*/

	/**
		* @function bucketCall - bucket interaction abstraction
		* @param {String} method
		* @param {Array} args - array of arguments to pass to the Couchbase method call
		* @param {Function} - callback: (error, result)
	*/
	bucketCall(method, args, callback) {
		try {
			const op = operation(this.retryOptions);
			op.attempt(() => {
				if(!['counter', 'query'].includes(method)) {
					this.collection[method](...args, (error, result, meta) => {
						error = mapErrorCodes(error);
						setTimeout(() => {
							if (error && [11, 16, 23].includes(error.code)) {
								if (typeof this.onReconnectCallback === 'function') {
									this.onReconnectCallback(null, `Attempt Couchbase reconnect to ${this.bucketName} bucket. Error Code: ${error.code} Bucket Connected Status: ${this.bucket.connected}`);
								}
								if (!op.retry(error)) {
									return callback(error, contentToValue(result), meta);
								}
							} else {
								return callback(error, contentToValue(result), meta);
							}
						}, (error && error.code === 11) ? 500 : 0);
					});
				} else if (method === 'counter') {
					return this.collection.binary().increment(...args , (error, result, meta) => {
						error = mapErrorCodes(error);
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
						}, (error && error.code === 11) ? 500 : 0);
					});
				} else if (method === 'query') {
					return this.cluster.query(...args, {}, (error, result, meta) => {
						error = mapErrorCodes(error);
						setTimeout(() => {
							if (error && [11, 16, 23].includes(error.code)) {
								if (typeof this.onReconnectCallback === 'function') {
									this.onReconnectCallback(null, `Attempt Couchbase reconnect to ${this.bucketName} bucket. Error Code: ${error.code} Bucket Connected Status: ${this.bucket.connected}`);
								}
								if (!op.retry(error)) {
									return callback(error, (result && isPopulatedObject(result) && result.hasOwnProperty('rows')) ? result.rows : result, meta);
								}
							} else {
								return callback(error, (result && isPopulatedObject(result) && result.hasOwnProperty('rows')) ? result.rows : result, meta);
							}
						}, (error && error.code === 11) ? 500 : 0);
					});
				} else {
					return this.bucket.viewQuery()(...args , (error, result, meta) => {
						error = mapErrorCodes(error);
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
						}, (error && error.code === 11) ? 500 : 0);
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
		* @function reconnectBucket - attempts to reconnect service to bucket
		* @param {Number} errorCode
		* @param {Function} - callback: (error)
	*/
	reconnectBucket(errorCode, callback) {
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

	/**
		* @function removeDuplicates - reduces Array to unique values
		* @param {Array} arr
		* @returns {Array}
	*/
	removeDuplicates(arr) {
		return Array.from(new Set(arr));
	}
}


/*
	HELPER FUNCTIONS -------------------------------------------------------------------------------------------------------------
*/

/**
	* @function mapErrorCodes - translates new error code to former number based code
	* @param {Object} error
	* @returns {Object}
*/
function mapErrorCodes(error) {
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
	* @function isPopulatedObject - checks if value is a populated Object or not
	* @param {Any} value
	* @returns {Boolean}
*/
function isPopulatedObject(value) {
	return (value && value instanceof Object && !Array.isArray(value) && Object.keys(value).length) ? true : false;
}

/**
	* @function isObjectWithProperty() - determines if the passed value is an Object, truthy, populated, and has target property
	* @param {any} - value
	* @returns {Boolean}
*/
function isObjectWithProperty(value, property) {
	return (isPopulatedObject(value) && property && typeof property === 'string' && value.hasOwnProperty(property)) ? true : false;
}

/**
	* @function valueFromResult - returns simplified data from result: content, value, or result itself
	* @param {Any} result
	* @returns {Any}
*/
function valueFromResult(result) {
	if(!isPopulatedObject(result)) {
		return result;
	} else {
		return ('content' in result) ? result.content : ('value' in result) ? result.value : result;
	}
}

/**
	* @function contentToValue - returns result with value (old style) instead of content (new style)
	* @param {Any} result
	* @returns {Any}
*/
function contentToValue(result) {
	if(
		isPopulatedObject(result) &&
		result.hasOwnProperty('content') &&
		!result.hasOwnProperty('value')
	) {
		return {
			value: result.content,
			cas: result.cas
		};
	}

	return result;
}


/**
	EXPORTS ---------------------------------------------------------------------------------------------------------------------------
*/

const privateFunctions = {
	mapErrorCodes,
	isPopulatedObject,
	isObjectWithProperty,
	valueFromResult,
	contentToValue
};

module.exports = typeof global.it !== 'function' ? CouchbaseService : Object.assign({ CouchbaseService }, privateFunctions);


// module.exports = CouchbaseService;
