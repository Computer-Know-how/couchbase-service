/*eslint no-console: 0*/

/*
	DEPENDENCIES --------------------------------------------------------------------------
*/

const { assert } = require('chai');

const mockery = require('mockery');

const {promisify} = require('util');

const sleep = promisify(setTimeout);

let CouchbaseService = null;// load manually w/mockery
let CBS = null;

const configOptions = {
	IPAddress: ['localhost:8091'],
	cluster: 'couchbase://localhost:8091',
	portNumber: '8091',
	bucket: 'default',
	atomicDocumentName: 'testCounter',
	auth: {
		username: 'Administrator',
		password: 'ckhadmin'
	},
	operationTimeout: 20000,
	onConnectCallback: async (error) => {
		if(error) {
			console.error(error);
			console.log('configOptions', configOptions);
			process.exit(1);
		} else {
			console.log('connected to Couchbase default bucket.');
		}
	},
	onReconnectCallback: (error, message) => {
		if(error) {
			console.error(error);
			console.log('configOptions', configOptions);
			console.log('Adjust configOptions to your local, live Coucbhase setup to run test suite');
			process.exit(1);
		}
		console.log(message);
	}
};


/*
	DESCRIBE: SETUP/TEARDOWN WRAPPER --------------------------------------------------------------------------
*/

function testSuite(testName, tests) {
	describe(testName, function(iteration=1) {
		this.timeout(150000);
		//SETUP
		before(async() => {
			console.log('BEFORE');
			try {
				//setup
				({ CouchbaseService } = require('../lib/index.js'));
				CBS = new CouchbaseService('default', configOptions);
				console.log('before sleep');
				await sleep(2000);
				console.log('after sleep');
				// for(let { key, value } of freshDocs()) {
				// 	await CBS.upsertPromise(key, value);
				// }
				// console.log('CBS', CBS);
				//undo
				// CouchbaseService = null;
			} catch(e) {
				console.log('e', e);
			}
		});

		beforeEach(() => {
			mockery.enable({ useCleanCache: true, warnOnUnregistered: false });
		});

		//TESTS
		tests(iteration);

		//TEARDOWN
		afterEach(() => {
			mockery.resetCache();
			mockery.disable();
		});

		after(() => {
			//tear down
			CouchbaseService = null;
			CBS = null;
		});
	});

}

/*
	TESTS -------------------------------------------------------------------------------------------------
*/

testSuite('GET tests', async (iteration) => {
	//getCallback()
	it(`${iteration++} getCallback() success test`, () => {
		const docName = 'thing::1';

		CBS.getCallback(docName, (error, result) => {
			assert.isNull(error);
			assert.deepEqual(result, freshDocs().find(fd => fd.key === docName).value);
		});
	});

	it(`${iteration++} getCallback() code 13 (doc not found) test`, () => {
		const docName = 'thing::42';

		CBS.getCallback(docName, (error, result) => {
			assert.isNotObject(error);
			assert.strictEqual(error.constructor.name, 'DocumentNotFoundError');
			assert.property(error, 'code');
			assert.equal(error.code, 13);
			assert.isNull(result);
		});
	});

	//getPromise()
	it(`${iteration++} getPromise() success test`, async () => {
		try {
			const docName = 'thing::1';

			const gp = await CBS.getPromise(docName);

			assert.deepEqual(gp, freshDocs().find(fd => fd.key === docName).value);
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++} getPromise() code 13 (doc not found) test`, async () => {
		try {
			const docName = 'thing::42';

			await CBS.getPromise(docName);

			throw new Error('Should NOT have succeeded!');
		} catch(e) {
			assert.strictEqual(e.constructor.name, 'DocumentNotFoundError');
			assert.property(e, 'code');
			assert.equal(e.code, 13);
		}
	});

	//getAndLockCallback()
	it(`${iteration++} getAndLockCallback() success test`, async () => {
		const docName = 'thing::1';

		CBS.getAndLockCallback(docName, 1, async (error, result) => {
			assert.isNull(error);
			assert.isObject(result);
			assert.hasAllKeys(result, ['value', 'cas']);
			assert.isNotEmpty(result.value);

			await CBS.unlockPromise(docName, result.cas);
		});
	});

	it(`${iteration++} getAndLockCallback() code 13 (doc not found) test`, async () => {
		CBS.getAndLockCallback('thing::42', 1, (error, result) => {
			assert.isNotObject(error);
			assert.strictEqual(error.constructor.name, 'DocumentNotFoundError');
			assert.property(error, 'code');
			assert.equal(error.code, 13);
			assert.isNull(result);
		});
	});

	//getAndLockPromise()
	it(`${iteration++} getAndLockPromise() success test`, async () => {
		try {
			const docName = 'thing::1';

			const gp = await CBS.getAndLockPromise(docName, 1);

			assert.hasAllKeys(gp, ['value', 'cas']);
			assert.deepEqual(gp.value, freshDocs().find(fd => fd.key === docName).value);
			assert.isObject(gp.cas);

			await CBS.unlockPromise(docName, gp.cas);
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++} getAndLockPromise() code 13 (doc not found) test`, async () => {
		try {
			const docName = 'thing::42';

			await CBS.getAndLockPromise(docName, 1);

			throw new Error('Should NOT have succeeded!');
		} catch(e) {
			assert.strictEqual(e.constructor.name, 'DocumentNotFoundError');
			assert.property(e, 'code');
			assert.equal(e.code, 13);
		}
	});

	//getMultiCallback()
	it(`${iteration++} getMultiCallback() full success test`, () => {
		const arDocNames = freshDocs().map(fd => fd.key);

		CBS.getMultiCallback(arDocNames, (error, result) => {
			assert.strictEqual(error, 0);
			assert.isObject(result);
			assert.isNotEmpty(result);
			for(let key in result) {
				assert.include(arDocNames, key);
				assert.deepEqual(result[key].value, freshDocs().find(fd => fd.key === key).value);
			}
		});
	});

	it(`${iteration++} getMultiCallback() mixed results test`, () => {
		const arDocNames = freshDocs().map(fd => fd.key).concat('thing::42');

		CBS.getMultiCallback(arDocNames, (error, result) => {
			assert.strictEqual(error, 1);
			assert.isObject(result);
			assert.isNotEmpty(result);
			for(let key in result) {
				if(key === 'thing::42') {
					assert.strictEqual(result[key].error.constructor.name, 'DocumentNotFoundError');
					assert.property(result[key].error, 'code');
					assert.equal(result[key].error.code, 13);
				} else {
					assert.include(arDocNames, key);
					assert.deepEqual(result[key].value, freshDocs().find(fd => fd.key === key).value);
				}
			}
		});
	});

	//getMultiPromise()
	it(`${iteration++} getMultiPromise() full success test`, async () => {
		try {
			const arDocNames = freshDocs().map(fd => fd.key);

			const { error, result } = await CBS.getMultiPromise(arDocNames);

			assert.strictEqual(error, 0);
			assert.isObject(result);
			assert.isNotEmpty(result);
			for(let key in result) {
				assert.include(arDocNames, key);
				assert.deepEqual(result[key].value, freshDocs().find(fd => fd.key === key).value);
			}
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++} getMultiPromise() mixed results test`, async () => {
		try {
			const arDocNames = freshDocs().map(fd => fd.key).concat('thing::42');

			const { error, result } = await CBS.getMultiPromise(arDocNames);

			assert.strictEqual(error, 1);
			assert.isObject(result);
			assert.isNotEmpty(result);
			for(let key in result) {
				if(key === 'thing::42') {
					assert.strictEqual(result[key].error.constructor.name, 'DocumentNotFoundError');
					assert.property(result[key].error, 'code');
					assert.equal(result[key].error.code, 13);
				} else {
					assert.include(arDocNames, key);
					assert.deepEqual(result[key].value, freshDocs().find(fd => fd.key === key).value);
				}
			}
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});
});// GET tests


testSuite('INSERT/UPSERT/REPLACE tests', async (iteration) => {
	//insertCallback()
	it(`${iteration++} insertCallback() success test`, () => {
		const { key, value } = oThing(4);

		CBS.insertCallback(key, value, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	it(`${iteration++} insertCallback() doc exists test`, () => {
		const { key, value } = oThing(4);

		CBS.insertCallback(key, value, (error, result) => {
			assert.strictEqual(error.constructor.name, 'DocumentExistsError');
			assert.isNull(result);
		});
	});

	//insertPromise()
	it(`${iteration++} insertPromise() success test`, async () => {
		const { key, value } = oThing(5);

		try {
			const ip = await CBS.insertPromise(key, value);

			assert.strictEqual(ip.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++} insertPromise() doc exists test`, async () => {
		const { key, value } = oThing(5);

		try {
			await CBS.insertPromise(key, value);

			throw new Error('Should NOT have succeeded!');
		} catch(e) {
			assert.strictEqual(e.constructor.name, 'DocumentExistsError');
		}
	});

	//upsertCallback()
	it(`${iteration++} upsertCallback() no cas success test`, () => {
		const { key, value } = oThing(4);

		CBS.upsertCallback(key, value, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	it(`${iteration++} upsertCallback() cas success test`, async () => {
		const { key } = oThing(4);

		const { value, cas } = await CBS.getAndLockPromise(key);

		CBS.upsertCallback(key, value, { cas }, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	//upsertPromise
	it(`${iteration++} upsertPromise() no cas success test`, async () => {
		try {
			const { key, value } = oThing(4);

			const up = await CBS.upsertPromise(key, value);

			assert.strictEqual(up.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++} upsertPromise() cas success test`, async () => {
		try {
			const { key } = oThing(4);
			const { value, cas } = await CBS.getAndLockPromise(key);

			const up = await CBS.upsertPromise(key, value, { cas });

			assert.strictEqual(up.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	//replaceCallback()
	it(`${iteration++} replaceCallback() cas success test`, async () => {
		const { key } = oThing(4);
		const { value, cas } = await CBS.getAndLockPromise(key);

		CBS.replaceCallback(key, value, { cas }, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	//replacePromise()
	it(`${iteration++} replacePromise() cas success test`, async () => {
		try {
			const { key } = oThing(4);
			const { value, cas } = await CBS.getAndLockPromise(key);

			const rp = await CBS.replacePromise(key, value, { cas });

			assert.strictEqual(rp.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});
});// INSERT/UPSERT/REPLACE tests


// testSuite('counterCallback tests', () => {
// 	it('counterCallback test', () => {
// 		return new Promise((resolve, reject) => {
// 			const cb = new CouchbaseService('test', configOptions);
// 			cb.collection.counterCallback((error, result) => {

// 				assert.isNull(error);
// 				assert.isObject(result);
// 				assert.equal(result.value, 2);

// 				return resolve(result);
// 			});
// 		});
// 	}).timeout(5000);
// }); // END counterCallback tests

// testSuite('touchCallback tests', () => {
// 	it('touchCallback test', () => {
// 		return new Promise((resolve, reject) => {
// 			const cb = new CouchbaseService('test', configOptions);
// 			cb.collection.touchCallback('test::1', 5, null, (error, result) => {

// 				assert.isNull(error);
// 				assert.isObject(result);
// 				assert.isObject(result.cas);
// 				return resolve(result);
// 			});
// 		});
// 	}).timeout(5000);
// }); // END touchCallback tests

// testSuite('removeCallback tests', () => {
// 	it('removeCallback test', () => {
// 		return new Promise((resolve, reject) => {
// 			const cb = new CouchbaseService('test', configOptions);
// 			cb.collection.removeCallback('test::1', null, (error, result) => {

// 				assert.isNull(error);
// 				assert.isObject(result);
// 				assert.isObject(result.cas);

// 				return resolve(result);
// 			});
// 		});
// 	}).timeout(5000);
// }); // END removeCallback tests



// testSuite('unlockCallback tests', () => {
// 	it('unlockCallback test', () => {
// 		return new Promise((resolve, reject) => {
// 			const cb = new CouchbaseService('test', configOptions);
// 			cb.collection.getAndLockCallback('test::1', 15, (error, result) => {
// 				const cas = result.cas;
// 				cb.collection.unlockCallback('test::1', cas, (error, result) => {
// 					assert.isNull(error);
// 					assert.isObject(result);
// 					assert.isObject(result.cas);

// 					return resolve(result);
// 				});
// 			});
// 		});
// 	}).timeout(5000);
// }); // END unlockCallback tests

// testSuite('viewQueryCallback tests', () => {
// 	it('viewQueryCallback test', () => {
// 		const cb = new CouchbaseService('test', configOptions);
// 		const ddoc = 'getAllNames';
// 		const name = 'getAllNames';
// 		const queryOptions = {
// 			limit: 10,
// 			skip: 0,
// 			order: 'descending'
// 		};
// 		cb.collection.viewQueryCallback(ddoc, name, queryOptions, (error, result) => {

// 			assert.isObject(result);
// 			assert.isArray(result.rows);
// 			assert.isObject(result.meta);
// 		});
// 	});
// }); // END viewQueryCallback tests


// testSuite('n1qlQueryCallback tests', () => {
// 	it('n1qlQueryCallback test', () => {
// 		return new Promise((resolve, reject) => {
// 			const cb = new CouchbaseService('test', configOptions);
// 			const n1ql = 'SELECT firstName from test where firstName LIKE Paul';
// 			cb.collection.n1qlQueryCallback(n1ql, (error, result) => {

// 				assert.isNull(error);
// 				assert.isArray(result);
// 				return resolve(result);
// 			});
// 		});
// 	}).timeout(5000);
// }); // END n1qlQueryCallback tests


// testSuite('insertPromise tests', () => {
// 	it('insertPromise test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.insertPromise('test::1', {firstName:'Paul', lastName: 'Rice'}, {});

// 			assert.isObject(result);
// 			assert.isObject(result.cas);
// 		} catch (e) {
// 			console.log(e);
// 		}
// 	}).timeout(5000);
// }); // END insertPromise tests

// testSuite('upsertPromise tests', () => {
// 	it('upsertPromise test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.upsertPromise('test::1', {firstName:'Rice', lastName: 'Paul'});

// 			assert.isObject(result);
// 			assert.property(result, 'cas');
// 			assert.isObject(result.cas);
// 		} catch (e) {
// 			console.log(e);
// 		}

// 	}).timeout(5000);
// });


// testSuite('getPromise tests', () => {
// 	it('getPromise test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.getPromise('test::1');

// 			assert.isObject(result);
// 			assert.property(result, 'firstName');
// 			assert.property(result, 'lastName');
// 			assert.equal(result.firstName, 'Paul');
// 			assert.equal(result.lastName, 'Rice');
// 		} catch (e) {
// 			console.log(e);
// 		}


// 	}).timeout(5000);
// }); // END getPromise tests


// testSuite('getAndLockPromise tests', () => {
// 	it('getAndLockPromise test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.getAndLockPromise('test::1', 15);

// 			assert.isObject(result);
// 			assert.property(result, 'cas');
// 			assert.isObject(result.cas);
// 			assert.property(result.value, 'firstName');
// 			assert.property(result.value, 'lastName');
// 			assert.equal(result.value.firstName, 'Paul');
// 			assert.equal(result.value.lastName, 'Rice');
// 		} catch (e) {
// 			console.log(e);
// 		}

// 	}).timeout(5000);
// }); // END getAndLockPromise tests

// testSuite('getMultiPromise tests', () => {
// 	it('getMultiPromise test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.getMultiPromise(['test::1', 'test::2']);
// 			assert.isObject(result.result['test::1']);
// 			assert.isObject(result.result['test::2']);
// 			assert.isObject(result.result['test::1'].cas);
// 			assert.isObject(result.result['test::2'].cas);
// 			assert.equal(result.result['test::1'].content.firstName, 'Paul');
// 			assert.equal(result.result['test::2'].content.firstName, 'Rice');
// 		} catch (e) {
// 			console.log(e);
// 		}

// 	}).timeout(5000);
// }); // END getMultiPromise tests

// testSuite('counterPromise tests', () => {
// 	it('counterPromise test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.counterPromise();

// 			assert.isObject(result);
// 			assert.isObject(result.cas);
// 			assert.isObject(result.token);
// 			assert.equal(result.value, 2);
// 		} catch (e) {
// 			console.log(e);
// 		}
// 	}).timeout(5000);
// }); // END counterPromise tests


// testSuite('removePromise tests', () => {
// 	it('removePromise test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.removePromise('test::1');

// 			assert.isObject(result);
// 			assert.isObject(result.cas)
// 		} catch (e) {
// 			console.log(e);
// 		}

// 	}).timeout(5000);
// }); // END removePromise tests


// testSuite('touchPromise tests', () => {
// 	it('touchPromise test', async  () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.touchPromise('test::1', 5);

// 			assert.isObject(result);
// 			assert.isObject(result.cas);

// 		} catch (e) {
// 			console.log(e);
// 		}
// 	}).timeout(5000);
// }); // END touchPromise tests

// testSuite('unlockPromise tests', () => {
// 	it('unlockPromise test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let getCas = await cb.collection.getAndLockPromise('test::1', 15);
// 			const cas = getCas.cas
// 			let result = await cb.collection.unlockPromise('test::1', cas);

// 			assert.isObject(result);
// 		} catch (e) {
// 			console.log(e);
// 		}
// 	}).timeout(5000);
// }); // END unlockPromise tests

// testSuite('viewQueryPromise tests', () => {
// 	it('viewQueryPromise test', async () => {
// 		const cb = new CouchbaseService('test', configOptions);
// 		const ddoc = 'getAllNames';
// 		const name = 'getAllNames';
// 		const queryOptions = {
// 			limit: 10,
// 			skip: 0,
// 			order: 'descending',
// 			descending: false,
// 			timeout: 10000
// 		};
// 		let result = await cb.collection.viewQueryPromise(ddoc, name, queryOptions);

// 		assert.isObject(result);
// 		assert.isArray(result.rows);
// 		assert.isObject(result.meta);
// 	}).timeout(5000);
// }); // END viewQueryPromise tests

// testSuite('n1qlQueryPromise tests', () => {
// 	it('n1qlQueryPromise test', async () => {
// 		const n1ql = 'SELECT firstName from test where firstName LIKE Paul';
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			let result = await cb.collection.n1qlQueryPromise(n1ql);

// 			assert.isArray(result);
// 		} catch (e) {
// 			console.log(e);
// 		}
// 	}).timeout(5000);
// }); // END n1qlQueryPromise tests

// testSuite('exists tests', () => {
// 	it('exists test', async () => {
// 		try {
// 			const cb = new CouchbaseService('test', configOptions);
// 			const key = 'test::1';
// 			let result = await cb.collection.exists(key);

// 			assert.isObject(result);
// 			assert.isObject(result.cas)
// 			assert.isBoolean(result.exists);
// 		} catch (e) {
// 			console.log(e);
// 		}
// 	}).timeout(5000);
// }); // END exists tests


/*
	HELPER FUNCTIONS ------------------------------------------------------------------------------------------------------
*/

function freshDocs() {
	return [
		oThing(1),
		oThing(2),
		oThing(3)
	];
}

function oThing(id=1) {
	return {
		key: `thing::${id}`,
		value: {
			bazinga: `${id}`,
			foo: `${id}`,
			doctype: 'thing'
		}
	};
}