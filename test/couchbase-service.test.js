/*
	DEPENDENCIES --------------------------------------------------------------------------
*/

const { assert } = require('chai');
const mockery = require('mockery');

let couchbaseService; // load manually w/mockery

const cbOptions = {
	cluster: 'couchbase://localhost:8091:8091',
	auth: {username: 'username', password: 'password'},
	atomicCounter: 'testAtomicCounter',
	operationTimeout: 10000
};


/*
	DESCRIBE: SETUP/TEARDOWN WRAPPER ------------------------------------------------
*/

function testSuite(testName, tests) {
	describe(testName, () => {
		//SETUP
		before(() => {
			mockery.registerSubstitute('couchbase', '../test/mocks/couchbase');
		});

		beforeEach(() => {
			mockery.enable({
				useCleanCache: true,
				warnOnUnregistered: false
			});

			mockery.registerAllowable('../lib/index.js');
			couchbaseService = require('../lib/index.js');
		});

		//TESTS
		tests();

		//TEARDOWN
		afterEach(() => {
			mockery.resetCache();
			mockery.disable();
		});

		after(() => {
			mockery.deregisterSubstitute('couchbase');
		});
	});
}


/*
	FUNCTIONS ------------------------------------------------------------------------------------------------------------------------------------------------
*/


testSuite('insertCallback tests', () => {
	it('insertCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.insertCallback('test::1', {firstName:'Paul', lastName: 'Rice'}, {}, (error, result) => {
			assert.isNull(error);
			assert.isObject(result);
			assert.equal(result.cas, 1545159314161860608);
		});
	});
}); // END insertCallback tests


testSuite('upsertCallback tests', () => {
	it('upsertCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.upsertCallback('test::1', {firstName:'Paul', lastName: 'Rice2'}, {}, (error, result) => {
			assert.isNull(error);
			assert.isObject(result);
			assert.equal(result.cas, 1545159314161860608);
		});
	});
}); // END upsertCallback tests


testSuite('getCallback tests', () => {
	it('getCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.getCallback('test::1', (error, result) => {
			assert.isNull(error);
			assert.isObject(result);
			assert.property(result, 'firstName');
			assert.property(result, 'lastName');
			assert.equal(result.firstName, 'Paul');
			assert.equal(result.lastName, 'Rice');
		});
	});
}); // END getCallback tests


testSuite('getAndLockCallback tests', () => {
	it('getAndLockCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.getAndLockCallback('test::1', 15, (error, result) => {
			assert.isNull(error);
			assert.isObject(result);
			assert.property(result.value, 'firstName');
			assert.property(result.value, 'lastName');
			assert.property(result, 'cas');
			assert.equal(result.value.firstName, 'Paul');
			assert.equal(result.value.lastName, 'Rice');
			assert.equal(result.cas, 1545229695764725760);
		});
	});
}); // END getAndLockCallback tests

testSuite('getMultiCallback tests', () => {
	it('getMultiCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.getMultiCallback(['test::1', 'test::2'], (error, result) => {
			assert.equal(error, 0);
			assert.isObject(result['test::1']);
			assert.isObject(result['test::2']);
			assert.equal(result['test::1'].value.firstName, 'Paul');
			assert.equal(result['test::2'].value.firstName, 'Rice');
		});
	});
}); // END getMultiCallback tests

testSuite('counterCallback tests', () => {
	it('counterCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.counterCallback((error, result) => {
			assert.isNull(error);
			assert.isObject(result);
			assert.equal(result.value, 2);
		});
	});
}); // END counterCallback tests


testSuite('removeCallback tests', () => {
	it('removeCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.removeCallback('test::1', (error) => {
			assert.isNull(error);
		});
	});
}); // END removeCallback tests


testSuite('touchCallback tests', () => {
	it('touchCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.touchCallback('test::1', 5, (error, result) => {
			assert.isNull(error);
			assert.isObject(result);
			assert.equal(result.cas, 1545240403128025088);
		});
	});
}); // END touchCallback tests

testSuite('unlockCallback tests', () => {
	it('unlockCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.unlockCallback('test::1', 1545242151479476224, (error, result) => {
			assert.isNull(error);
			assert.isNull(result);
		});
	});
}); // END unlockCallback tests

testSuite('viewQueryCallback tests', () => {
	it('viewQueryCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		const ddoc = 'getAllNames';
		const name = 'getAllNames';
		const queryOptions = {
			limit: 10,
			skip: 0,
			order: 'descending'
		};
		cb.viewQueryCallback(ddoc, name, queryOptions, (error, result) => {
			assert.isNull(error);
			assert.isArray(result);
		});
	});
}); // END viewQueryCallback tests


testSuite('n1qlQueryCallback tests', () => {
	it('n1qlQueryCallback test', () => {
		const cb = new couchbaseService('test', cbOptions);
		const n1ql = 'SELECT firstName from test where firstName LIKE Paul';
		cb.n1qlQueryCallback(n1ql, (error, result) => {
			assert.isNull(error);
			assert.isArray(result);
		});
	});
}); // END n1qlQueryCallback tests


testSuite('insertPromise tests', () => {
	it('insertPromise test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.insertPromise('test::1', {firstName:'Paul', lastName: 'Rice'}, {}).then((result) => {
			assert.isObject(result);
			assert.equal(result.cas, 1545159314161860600);
		}).catch(e => {
			assert.isNull(e);
		});
	});
}); // END insertPromise tests


testSuite('upsertPromise tests', () => {
	it('upsertPromise test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.upsertPromise('test::1', {firstName:'Rice', lastName: 'Paul'}, {}).then((result) => {
			assert.isObject(result);
			assert.equal(result.cas, 1545159314161860600);
		}).catch(e => {
			assert.isNull(e);
		});
	});
}); // END upsertPromise tests


testSuite('getPromise tests', () => {
	it('getPromise test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.getPromise('test::1').then((result) => {
			assert.isObject(result);
			assert.property(result, 'firstName');
			assert.property(result, 'lastName');
			assert.equal(result.firstName, 'Paul');
			assert.equal(result.lastName, 'Rice');
		}).catch(e => {
			assert.isNull(e);
		});
	});
}); // END getPromise tests


testSuite('getAndLockPromise tests', () => {
	it('getAndLockPromise test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.getAndLockPromise('test::1', 15).then((result) => {
			assert.isObject(result);
			assert.property(result.value, 'firstName');
			assert.property(result.value, 'lastName');
			assert.property(result, 'cas');
			assert.equal(result.value.firstName, 'Paul');
			assert.equal(result.value.lastName, 'Rice');
			assert.equal(result.cas, 1545229695764725760);
		}).catch(e => {
			assert.isNull(e);
		});
	});
}); // END getAndLockPromise tests

testSuite('getMultiPromise tests', () => {
	it('getMultiPromise test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.getMultiPromise(['test::1', 'test::2']).then((result) => {
			assert.isObject(result['test::1']);
			assert.isObject(result['test::2']);
			assert.equal(result['test::1'].value.firstName, 'Paul');
			assert.equal(result['test::2'].value.firstName, 'Rice');
		}).catch(e => {
			assert.isNull(e);
		});
	});
}); // END getMultiPromise tests

testSuite('counterPromise tests', () => {
	it('counterPromise test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.counterPromise().then((result) => {
			assert.isObject(result);
			assert.equal(result.value, 2);
		}).catch(e => {
			assert.isNull(e);
		});
	});
}); // END counterPromise tests


testSuite('removePromise tests', () => {
	it('removePromise test', () => {
		const cb = new couchbaseService('test', cbOptions);
		cb.removePromise('test::1').then((result) => {
			assert.isNull(result);
		}).catch(e => {
			assert.isNull(e);
		});
	});
}); // END removePromise tests
