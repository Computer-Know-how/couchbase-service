/*eslint no-console: 0*/
/*
	DEPENDENCIES --------------------------------------------------------------------------------
*/


/*
	COUCHBASE DSN MOCK OBJECT -------------------------------------------
*/

function Cluster(ipLocation) {
	this.ipLocation = ipLocation;

	this.authenticate = (username, password) => {
		if(!username || !password) {
			console.error(`Couchbase authentication failed. Credentials submitted, username: ${username}, password: ${password}`);
		}
	};

	this.openBucket = (bucketName, callback) => {
		return {
			operationTimeout: 10000,
			bucketName:bucketName,
			atomicCounter: 'testAtomicCounter',
			onReconnectCallback: (error) => {
				if(error) {
					throw error;
				}
			},
			connected: true,
			manager: () => {
				return {
					insertDesignDocument: (viewName, dataStruct, callback) => {
						return callback(null);
					},
					upsertDesignDocument: (viewName, dataStruct, callback) => {
						return callback(null);
					}
				} ;
			},
			counter: (documentName, delta, options, callback) => {
				return callback(null, {
					cas: 1545239775609749504,
					token: undefined,
					value: 2
				});
			},
			insert: (documentName, listingData, options, callback) => {
				return callback(null, { cas: 1545159314161860608});
			},
			upsert: (documentName, listingData, options, callback) => {
				return callback(null, { cas: 1545159314161860608});
			},
			remove: (documentName, options, callback) => {
				return callback(null, null);
			},
			touch: (documentName, expiryDate, options, callback) => {
				return callback(null, { cas: 1545240403128025088 });
			},
			get: (documentName, callback) => {
				if(documentName === 'test::1') {
					return callback(null, {value: {firstName:'Paul', lastName: 'Rice'}});
				} else  if(documentName === 'test::2') {
					return callback(null, {value: {firstName:'Rice', lastName: 'Paul'}});
				} else {
					return callback(null, {value: {firstName:'some', lastName: 'error'}});
				}
			},
			getMulti: (docIDs, callback) => {
				console.log('i do not think we get here');
				return callback(0, {
					'test::d1':{
						'value': {
							'doctype': 'enterprise'
						}
					},
					'test::d2':{
						'value': {
							'doctype': 'enterprise'
						}
					}
				});
			},
			getAndLock: (docID, options, callback) => {
				return callback(null, { cas: 1545229695764725760, value: { firstName: 'Paul', lastName: 'Rice' } });
			},
			unlock: (docName, cas, callback) => {
				return callback(null, null);
			},
			query: (query, callback) => {//callback: (error, result, meta)
				let result = [];
				if(query.ddoc === 'getAllNames') {
					result = [{
						key: '2018-12-19T20:43:30.762Z',
						value: '20887014',
						id: 'test::1'
					},
					{
						key: '2018-12-19T20:43:30.761Z',
						value: '20886982',
						id: 'test::2'
					}];
				}

				return callback(null, result);
			}
		};
	};
}


/**
	* ViewQuery mock
	* generates: {
	*		ddoc: 'data_lake_TCAID_by_customerID',
	*		name: 'data_lake_TCAID_by_customerID',
	*		options: { descending: true, stale: 'false', key: '"2683448"' } || { descending: true, limit: '20', skip: '0', stale: 'false' },,
	*		postoptions: {}
	*	}
*/
function ViewQuery() {
	this.ddoc = null;
	this.name = null;
	this.options = {};
	this.postoptions = {};
}
ViewQuery.Update = {
	BEFORE: 1,
	NONE: 2,
	AFTER: 3
};
ViewQuery.Order = {
	ASCENDING: 1,
	DESCENDING: 2
};
ViewQuery.prototype.from = function(ddoc, name) {
	this.ddoc = ddoc;
	this.name = name;
	return this;
};
ViewQuery.prototype.stale = function(stale) {
	if(stale === ViewQuery.Update.BEFORE) {
		this.options.stale = 'false';
	} else if(stale === ViewQuery.Update.NONE) {
		this.options.stale = 'ok';
	} else if(stale === ViewQuery.Update.AFTER) {
		this.options.stale = 'update_after';
	} else {
		throw new TypeError('invalid option passed.');
	}
	return this;
};
ViewQuery.prototype.limit = function(limit) {
	this.options.limit = limit;
	return this;
};
ViewQuery.prototype.order = function(order) {
	if(order === ViewQuery.Order.ASCENDING) {
		this.options.descending = false;
	} else if(order === ViewQuery.Order.DESCENDING) {
		this.options.descending = true;
	} else {
		throw new TypeError('invalid option passed.');
	}
	return this;
};
ViewQuery.prototype.skip = function(skip) {
	this.options.skip = skip;
	return this;
};
ViewQuery.prototype.key = function(key) {
	this.options.key = JSON.stringify(key);
	return this;
};
ViewQuery.prototype.keys = function(keys) {
	this.postoptions.keys = keys;
	return this;
};
ViewQuery.prototype.range = function(start, end, inclusive_end) {
	this.options.startkey = JSON.stringify(start);
	this.options.endkey = JSON.stringify(end);
	if(inclusive_end) {
		this.options.inclusive_end = 'true';
	} else {
		delete this.options.inclusive_end;
	}
	return this;
};
ViewQuery.from = function(ddoc, name) {
	return (new ViewQuery()).from(ddoc, name);
};


function N1qlQuery() {}
N1qlQuery.prototype.fromString = function(n1ql) {
	this.n1ql = n1ql;
	return this;
};
N1qlQuery.fromString = function(n1ql) {
	return (new N1qlQuery()).fromString(n1ql);
};
/*
	EXPORTS ----------------------------------------------------------------------------
*/

module.exports = {
	Cluster,
	ViewQuery,
	N1qlQuery
};