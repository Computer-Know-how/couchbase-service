/*
	DEPENDENCIES --------------------------------------------------------------------------------
*/
/*
	COUCHBASE DSN MOCK OBJECT -------------------------------------------
*/
function Cluster(ipLocation) {
	this.ipLocation = ipLocation;
	this.bucket = (bucketName, callback) => {
			return {
				defaultCollection: () => {
					return {
						insertCallback: (key, value, options, callback) => {
							return callback(null, { cas: {}});
						},
						upsertCallback: (key, value, options, callback) => {
							return callback(null, { cas: {}});
						},
						getCallback: (key, callback) => {
							return callback('test::1', {firstName:'Paul', lastName: 'Rice2'});
						},
						getAndLockCallback: (key, lockTime, callback) => {
							return callback(null, {cas: {}, value: {firstName: 'Paul', lastName: 'Rice2' }});
						},
						counterCallback: (callback) => {
							return callback(null, { cas: {}, token: {}, value: 2});
						},
						touchCallback: (key, expiry, options, callback) => {
							return callback(null, { cas: {}});
						},
						removeCallback: (key, options, callback) => {
							return callback(null, { cas: {}});
						},
						insertPromise: (key, value, options={}) => {
							return callback({ cas: {}, token: {}})
						},
						upsertPromise: (key, value, options={}) => {
							return callback({ cas: {}, token: {}})
						},
						getPromise: (key) => {
							return { firstName: 'Paul', lastName: 'Rice' }
						},
						getAndLockPromise: (key, timeout) => {
							return {cas:{},value: { firstName: 'Paul', lastName: 'Rice' }}
						},
						getMultiPromise: (key) => {
							return {
									error: 0,
									result: {
									'test::1': {
										cas: {},
										content: { firstName: 'Paul', lastName: 'Rice' }
									},
									'test::2': {
										cas: {},
										content: { firstName: 'Rice', lastName: 'Paul' }
									}
								}
							}
						},
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
						getMultiCallback: (docIDs, callback) => {
							return callback(0, {
								'test::1':{
									'value': {
										'doctype': 'enterprise',
										'firstName': 'Paul'
									}
								},
								'test::2':{
									'value': {
										'doctype': 'enterprise',
										'firstName': 'Paul'
									}
								}
							});
						},
						viewQueryPromise: (ddoc, name, options) => {
							return {
								meta: { totalRows: 2 },
								rows: [
										{ value: null, id: 'test::1', key: 'test::1' },
										{ value: null, id: 'test::2', key: 'test::2' }
									]
								}
						},
						viewQueryCallback: (ddoc, name, options, callback) => {
							return callback(null,{
								meta: { totalRows: 2 },
								rows: [
										{ value: null, id: 'test::1', key: 'test::1' },
										{ value: null, id: 'test::2', key: 'test::2' }
									]
								});
						},
						exists: (key) => {
							return {cas: {}, exists: true}
						},
						unlockCallback: (key, cas, callback) => {
							return callback(null, { cas: {}});
						},
						unlockPromise: (key, cas) => {
							return {cas: {}};
						},
						counterPromise: () => {
							return({cas: {}, token: {},value: 2
							});
						},
						removePromise: (key, options={}) => {
							return { cas: {}};
						},
						touchPromise: (key, options={}) => {
							return { cas: {}};
						},
						n1qlQueryPromise: (qry) => {
							return [ { firstName: 'Paul' } ];
						},
						n1qlQueryCallback: (qry, callback) => {
							return callback(null, [ { firstName: 'Paul' } ]);
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
						},
						binary: () => {
							return { counter: (documentName, delta, options, callback) => {
								return callback(null, {
									cas: 1545239775609749504,
									token: undefined,
									value: 2
								});
							},
						}
						},
					};
				},


					operationTimeout: 10000,
					bucketName:bucketName,
					atomicCounter: 'testAtomicCounter',
					onReconnectCallback: (error) => {
						if(error) {
							throw error;
						}
					},
					connected: true,

			}
		// this.collection =


	};
};


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