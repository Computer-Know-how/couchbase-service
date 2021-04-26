/*
	DEPENDENCIES --------------------------------------------------------------------------
*/
const { assert } = require('chai');
const mockery = require('mockery');
const {promisify} = require('util');
const sleep = promisify(setTimeout);



let couchbaseService; // load manually w/mockery

const configOptions = {
	IPAddress: ["localhost:8091"],
	portNumber: "8091",
	bucket: 'test',
	atomicDocumentName: "testCounter",
	auth: {
		username: "Administrator",
		password: "password"
	},
	operationTimeout: 20000
};




function testSuite(testName, tests) {
	describe(testName, () => {
		//SETUP
		before(() => {
			mockery.registerSubstitute('couchbase', '../test/mocks/couchbase');
		});

		beforeEach( async () => {
			mockery.enable({
				useCleanCache: true,
				warnOnUnregistered: false
			});
			mockery.registerAllowable('../lib/index.js');
			couchbaseService = require('../lib/index.js');
			await sleep(1000);
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



testSuite('insertCallback tests', () => {
	it('insertCallback test', async () => {
			return new Promise((resolve, reject) => {
				const cb = new couchbaseService('test', configOptions);
				cb.collection.insertCallback('test::1', {firstName:'Paul', lastName: 'Rice'}, null,(error, result) => {

					assert.isObject(result);
					assert.isNull(error);
					assert.isObject(result.cas);

					return resolve(result);
				});
			});
	}).timeout(10000);
}); // END insertCallback tests

testSuite('upsertCallback tests', () => {
	it('upsertCallback test', () => {
		return new Promise((resolve, reject) => {
			const cb = new couchbaseService('test', configOptions);
				cb.collection.upsertCallback('test::1', {firstName:'Paul', lastName: 'Rice2'}, null,(error, result) => {

				assert.isNull(error);
				assert.exists(result);
				assert.isObject(result);
				assert.isObject(result.cas);

				return resolve(result);
			});
		});
	}).timeout(10000);
}); // END upsertCallback tests


testSuite('getCallback tests', () => {
	it('getCallback test', () => {
		return new Promise((resolve, reject) => {
			const cb = new couchbaseService('test', configOptions);
			cb.collection.getCallback('test::1', (error, result) => {

				assert.isObject(result);
				assert.property(result, 'firstName');
				assert.property(result, 'lastName');
				assert.equal(result.firstName, 'Paul');
				assert.equal(result.lastName, 'Rice2');

				return resolve(result);
			});
		});
	}).timeout(5000);
}); // END getCallback tests


testSuite('getAndLockCallback tests', () => {
	it('getAndLockCallback test', () => {
		try {
			return new Promise((resolve, reject) => {
				const cb = new couchbaseService('test', configOptions);
				cb.collection.getAndLockCallback('test::1', 15, (error, result) => {

					assert.isObject(result);
					assert.property(result.value, 'firstName');
					assert.property(result.value, 'lastName');
					assert.property(result, 'cas');
					assert.equal(result.value.firstName, 'Paul');
					assert.equal(result.value.lastName, 'Rice2');
					assert.isObject(result.cas);

					return resolve(result);
				});
			});

		} catch (e) {
			console.log(e);
		}
	}).timeout(5000);
}); // END getAndLockCallback tests

testSuite('getMultiCallback tests', () => {
	it('getMultiCallback test', () => {
		return new Promise((resolve, reject) => {
			const cb = new couchbaseService('test', configOptions);
			cb.collection.getMultiCallback(['test::1', 'test::2'], (error, result) => {

				assert.equal(error, 0);
				assert.isObject(result['test::1']);
				assert.isObject(result['test::2']);
				assert.equal(result['test::1'].value.firstName, 'Paul');
				assert.equal(result['test::2'].value.firstName, 'Paul');

				return resolve(result)
			});
		});
	}).timeout(5000);
}); // END getMultiCallback tests

testSuite('counterCallback tests', () => {
	it('counterCallback test', () => {
		return new Promise((resolve, reject) => {
			const cb = new couchbaseService('test', configOptions);
			cb.collection.counterCallback((error, result) => {

				assert.isNull(error);
				assert.isObject(result);
				assert.equal(result.value, 2);

				return resolve(result)
			});
		});
	}).timeout(5000);
}); // END counterCallback tests

testSuite('touchCallback tests', () => {
	it('touchCallback test', () => {
		return new Promise((resolve, reject) => {
			const cb = new couchbaseService('test', configOptions);
			cb.collection.touchCallback('test::1', 5, null, (error, result) => {

				assert.isNull(error);
				assert.isObject(result);
				assert.isObject(result.cas);
				return resolve(result)
			});
		});
	}).timeout(5000);
}); // END touchCallback tests

testSuite('removeCallback tests', () => {
	it('removeCallback test', () => {
		return new Promise((resolve, reject) => {
			const cb = new couchbaseService('test', configOptions);
			cb.collection.removeCallback('test::1', null, (error, result) => {

				assert.isNull(error);
				assert.isObject(result);
				assert.isObject(result.cas);

				return resolve(result)
			});
		});
	}).timeout(5000);
}); // END removeCallback tests



testSuite('unlockCallback tests', () => {
	it('unlockCallback test', () => {
		return new Promise((resolve, reject) => {
			const cb = new couchbaseService('test', configOptions);
			cb.collection.getAndLockCallback('test::1', 15, (error, result) => {
				const cas = result.cas;
				cb.collection.unlockCallback('test::1', cas, (error, result) => {
					assert.isNull(error);
					assert.isObject(result);
					assert.isObject(result.cas);

					return resolve(result)
				});
			});
		});
	}).timeout(5000);
}); // END unlockCallback tests

testSuite('viewQueryCallback tests', () => {
	it('viewQueryCallback test', () => {
		const cb = new couchbaseService('test', configOptions);
		const ddoc = 'getAllNames';
		const name = 'getAllNames';
		const queryOptions = {
			limit: 10,
			skip: 0,
			order: 'descending'
		};
		cb.collection.viewQueryCallback(ddoc, name, queryOptions, (error, result) => {

		assert.isObject(result);
		assert.isArray(result.rows);
		assert.isObject(result.meta)
		});
	});
}); // END viewQueryCallback tests


testSuite('n1qlQueryCallback tests', () => {
	it('n1qlQueryCallback test', () => {
		return new Promise((resolve, reject) => {
		const cb = new couchbaseService('test', configOptions);
		const n1ql = 'SELECT firstName from test where firstName LIKE Paul';
		cb.collection.n1qlQueryCallback(n1ql, (error, result) => {

				assert.isNull(error);
				assert.isArray(result);
				return resolve(result)
			});
		});
	}).timeout(5000);
}); // END n1qlQueryCallback tests


testSuite('insertPromise tests', () => {
	it('insertPromise test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.insertPromise('test::1', {firstName:'Paul', lastName: 'Rice'}, {});

			assert.isObject(result);
			assert.isObject(result.cas);
		} catch (e) {
			console.log(e);
		}
	}).timeout(5000);
}); // END insertPromise tests

testSuite('upsertPromise tests', () => {
	it('upsertPromise test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.upsertPromise('test::1', {firstName:'Rice', lastName: 'Paul'});

			assert.isObject(result);
			assert.property(result, 'cas');
			assert.isObject(result.cas);
		} catch (e) {
			console.log(e);
		}

	}).timeout(5000);
});


testSuite('getPromise tests', () => {
	it('getPromise test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.getPromise('test::1');

			assert.isObject(result);
			assert.property(result, 'firstName');
			assert.property(result, 'lastName');
			assert.equal(result.firstName, 'Paul');
			assert.equal(result.lastName, 'Rice');
		} catch (e) {
			console.log(e);
		}


	}).timeout(5000);
}); // END getPromise tests


testSuite('getAndLockPromise tests', () => {
	it('getAndLockPromise test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.getAndLockPromise('test::1', 15);

			assert.isObject(result);
			assert.property(result, 'cas');
			assert.isObject(result.cas);
			assert.property(result.value, 'firstName');
			assert.property(result.value, 'lastName');
			assert.equal(result.value.firstName, 'Paul');
			assert.equal(result.value.lastName, 'Rice');
		} catch (e) {
			console.log(e);
		}

	}).timeout(5000);
}); // END getAndLockPromise tests

testSuite('getMultiPromise tests', () => {
	it('getMultiPromise test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.getMultiPromise(['test::1', 'test::2']);
			assert.isObject(result.result['test::1']);
			assert.isObject(result.result['test::2']);
			assert.isObject(result.result['test::1'].cas);
			assert.isObject(result.result['test::2'].cas);
			assert.equal(result.result['test::1'].content.firstName, 'Paul');
			assert.equal(result.result['test::2'].content.firstName, 'Rice');
		} catch (e) {
			console.log(e);
		}

	}).timeout(5000);
}); // END getMultiPromise tests

testSuite('counterPromise tests', () => {
	it('counterPromise test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.counterPromise();

			assert.isObject(result);
			assert.isObject(result.cas);
			assert.isObject(result.token);
			assert.equal(result.value, 2);
		} catch (e) {
			console.log(e);
		}
	}).timeout(5000);
}); // END counterPromise tests


testSuite('removePromise tests', () => {
	it('removePromise test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.removePromise('test::1');

			assert.isObject(result);
			assert.isObject(result.cas)
		} catch (e) {
			console.log(e);
		}

	}).timeout(5000);
}); // END removePromise tests


testSuite('touchPromise tests', () => {
	it('touchPromise test', async  () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.touchPromise('test::1', 5);

			assert.isObject(result);
			assert.isObject(result.cas);

		} catch (e) {
			console.log(e);
		}
	}).timeout(5000);
}); // END touchPromise tests

testSuite('unlockPromise tests', () => {
	it('unlockPromise test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			let getCas = await cb.collection.getAndLockPromise('test::1', 15);
			const cas = getCas.cas
			let result = await cb.collection.unlockPromise('test::1', cas);

			assert.isObject(result);
		} catch (e) {
			console.log(e);
		}
	}).timeout(5000);
}); // END unlockPromise tests

testSuite('viewQueryPromise tests', () => {
	it('viewQueryPromise test', async () => {
		const cb = new couchbaseService('test', configOptions);
		const ddoc = 'getAllNames';
		const name = 'getAllNames';
		const queryOptions = {
			limit: 10,
			skip: 0,
			order: 'descending',
			descending: false,
			timeout: 10000
		};
		let result = await cb.collection.viewQueryPromise(ddoc, name, queryOptions);

		assert.isObject(result);
		assert.isArray(result.rows);
		assert.isObject(result.meta);
	}).timeout(5000);
}); // END viewQueryPromise tests

testSuite('n1qlQueryPromise tests', () => {
	it('n1qlQueryPromise test', async () => {
		const n1ql = 'SELECT firstName from test where firstName LIKE Paul';
		try {
			const cb = new couchbaseService('test', configOptions);
			let result = await cb.collection.n1qlQueryPromise(n1ql);

			assert.isArray(result);
		} catch (e) {
			console.log(e);
		}
	}).timeout(5000);
}); // END n1qlQueryPromise tests

testSuite('exists tests', () => {
	it('exists test', async () => {
		try {
			const cb = new couchbaseService('test', configOptions);
			const key = 'test::1';
			let result = await cb.collection.exists(key);

			assert.isObject(result);
			assert.isObject(result.cas)
			assert.isBoolean(result.exists);
		} catch (e) {
			console.log(e);
		}
	}).timeout(5000);
}); // END exists tests



