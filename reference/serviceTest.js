/*eslint no-console: 0*/

/*
	DEPENDENCIES --------------------------------------------------------------------------
*/

const { assert } = require('chai');

const { promisify } = require('util');

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
		password: ''//put your password here
	},
	operationTimeout: 20000,
	onConnectCallback: async (error) => {
		if(error) {
			console.log('Failed to connect to Couchbase. Please check settings and make sure you have an instance.');
			process.exit(1);
		} else {
			console.log('connected to Couchbase default bucket.');
		}
	},
	onReconnectCallback: (error, message) => {
		if(error) {
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
			try {
				//setup
				({ CouchbaseService } = require('../lib/index.js'));
				CBS = new CouchbaseService('default', configOptions);
				await sleep(2000);
				if(testName === 'GET tests') {
					for(let { key, value } of freshDocs()) {
						await CBS.upsertPromise(key, value);
					}
				}
			} catch(e) {
				console.log('e', e);
			}
		});

		beforeEach(() => {});

		//TESTS
		tests(iteration);

		//TEARDOWN
		afterEach(() => {});

		after(() => {});
	});
}

/*
	TESTS -------------------------------------------------------------------------------------------------
*/

testSuite('GET tests', async (iteration) => {
	//getCallback()
	it(`${iteration++}. getCallback() success test`, () => {
		const docName = 'thing::1';

		CBS.getCallback(docName, (error, result) => {
			assert.isNull(error);
			assert.deepEqual(result, freshDocs().find(fd => fd.key === docName).value);
		});
	});

	it(`${iteration++}. getCallback() code 13 (doc not found) test`, () => {
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
	it(`${iteration++}. getPromise() success test`, async () => {
		try {
			const docName = 'thing::1';

			const gp = await CBS.getPromise(docName);

			assert.deepEqual(gp, freshDocs().find(fd => fd.key === docName).value);
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++}. getPromise() code 13 (doc not found) test`, async () => {
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
	it(`${iteration++}. getAndLockCallback() success test`, async () => {
		const docName = 'thing::1';

		CBS.getAndLockCallback(docName, 1, async (error, result) => {
			assert.isNull(error);
			assert.isObject(result);
			assert.hasAllKeys(result, ['value', 'cas']);
			assert.isNotEmpty(result.value);

			await CBS.unlockPromise(docName, result.cas);
		});
	});

	it(`${iteration++}. getAndLockCallback() code 13 (doc not found) test`, async () => {
		CBS.getAndLockCallback('thing::42', 1, (error, result) => {
			assert.isNotObject(error);
			assert.strictEqual(error.constructor.name, 'DocumentNotFoundError');
			assert.property(error, 'code');
			assert.equal(error.code, 13);
			assert.isNull(result);
		});
	});

	//getAndLockPromise()
	it(`${iteration++}. getAndLockPromise() success test`, async () => {
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

	it(`${iteration++}. getAndLockPromise() code 13 (doc not found) test`, async () => {
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
	it(`${iteration++}. getMultiCallback() full success test`, () => {
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

	it(`${iteration++}. getMultiCallback() mixed results test`, () => {
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
	it(`${iteration++}. getMultiPromise() full success test`, async () => {
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

	it(`${iteration++}. getMultiPromise() mixed results test`, async () => {
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
	it(`${iteration++}. insertCallback() success test`, () => {
		const { key, value } = oThing(4);

		CBS.insertCallback(key, value, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	it(`${iteration++}. insertCallback() doc exists test`, () => {
		const { key, value } = oThing(4);

		CBS.insertCallback(key, value, (error, result) => {
			assert.strictEqual(error.constructor.name, 'DocumentExistsError');
			assert.isNull(result);
		});
	});

	//insertPromise()
	it(`${iteration++}. insertPromise() success test`, async () => {
		const { key, value } = oThing(5);

		try {
			const ip = await CBS.insertPromise(key, value);

			assert.strictEqual(ip.constructor.name, 'MutationResult');
		} catch(e) {
			assert.strictEqual(e.constructor.name, 'DocumentExistsError');
		}
	});

	it(`${iteration++}. insertPromise() doc exists test`, async () => {
		const { key, value } = oThing(5);

		try {
			await CBS.insertPromise(key, value);

			throw new Error('Should NOT have succeeded!');
		} catch(e) {
			assert.strictEqual(e.constructor.name, 'DocumentExistsError');
		}
	});

	//upsertCallback()
	it(`${iteration++}. upsertCallback() no cas (upsert) success test`, () => {
		const { key, value } = oThing(4);

		CBS.upsertCallback(key, value, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	it(`${iteration++}. upsertCallback() cas (replace) success test`, async () => {
		const { key } = oThing(4);

		const { value, cas } = await CBS.getAndLockPromise(key);

		CBS.upsertCallback(key, value, { cas }, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	//upsertPromise
	it(`${iteration++}. upsertPromise() no cas (upsert) success test`, async () => {
		try {
			const { key, value } = oThing(4);

			const up = await CBS.upsertPromise(key, value);

			assert.strictEqual(up.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++}. upsertPromise() cas (replace) success test`, async () => {
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
	it(`${iteration++}. replaceCallback() cas success test`, async () => {
		const { key } = oThing(4);
		const { value, cas } = await CBS.getAndLockPromise(key);

		CBS.replaceCallback(key, value, { cas }, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	//replacePromise()
	it(`${iteration++}. replacePromise() cas success test`, async () => {
		try {
			const { key } = oThing(4);
			const { value, cas } = await CBS.getAndLockPromise(key);

			const rp = await CBS.replacePromise(key, value, { cas });

			assert.strictEqual(rp.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});
});// END INSERT/UPSERT/REPLACE tests


testSuite('REMOVE/UNLOCK/TOUCH tests', async (iteration) => {
	//removeCallback()
	it(`${iteration++}. removeCallback() success test`, async () => {
		const { key, value } = oThing(4);
		await CBS.upsertPromise(key, value);

		CBS.removeCallback(key, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	it(`${iteration++}. removeCallback() cas success test`, async () => {
		const { key, value } = oThing(4);
		await CBS.upsertPromise(key, value);
		const { cas } = await CBS.getAndLockPromise(key, 2);

		CBS.removeCallback(key, { cas }, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	//removePromise()
	it(`${iteration++}. removePromise() success test`, async () => {
		try {
			const { key, value } = oThing(4);
			await CBS.upsertPromise(key, value);

			const rp = await CBS.removePromise(key);

			assert.strictEqual(rp.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++}. removePromise() cas success test`, async () => {
		try {
			const { key, value } = oThing(4);
			await CBS.upsertPromise(key, value);
			const { cas } = await CBS.getAndLockPromise(key, 2);

			const rp = await CBS.removePromise(key, { cas });

			assert.strictEqual(rp.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	//unlockCallback()
	it(`${iteration++}. unlockCallback() success test`, async () => {
		const { key } = oThing(1);
		const { cas } = await CBS.getAndLockPromise(key, 2);

		CBS.unlockCallback(key, cas, (error) => {
			assert.isNull(error);
		});
	});

	it(`${iteration++}. unlockCallback() failure test`, async () => {
		const { key } = oThing(1);
		const { cas } = await CBS.getAndLockPromise(key, 1);

		CBS.unlockCallback(key, { cas:'1234' }, (error) => {
			assert.include(error.message, 'bad cas passed');
			CBS.unlockCallback(key, cas, (error) => {
				assert.isNull(error);
			});
		});
	});

	//unlockPromise()
	it(`${iteration++}. unlockPromise() success test`, async () => {
		const { key } = oThing(1);
		const { cas } = await CBS.getAndLockPromise(key, 2);

		try {
			const up = await CBS.unlockPromise(key, cas);

			assert.isNull(up);
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++}. unlockPromise() failure test`, async () => {
		const { key } = oThing(1);
		const { cas } = await CBS.getAndLockPromise(key, 1);

		try {
			await CBS.unlockPromise(key);

			throw new Error('should NOT have succeeded');
		} catch(e) {
			assert.include(e.message, 'invalid arguments');

			try {
				const up = await CBS.unlockPromise(key, cas);

				assert.isNull(up);
			} catch(e) {
				throw new Error(`should NOT have failed: ${e.message}`);
			}
		}
	});

	//touchCallback()
	it(`${iteration++}. touchCallback() success test`, async () => {
		const { key } = oThing(1);

		CBS.touchCallback(key, 0, (error, result) => {
			assert.isNull(error);
			assert.strictEqual(result.constructor.name, 'MutationResult');
		});
	});

	it(`${iteration++}. touchCallback() failure test`, async () => {
		const { key } = oThing(7);

		CBS.touchCallback(key, 0, (error, result) => {
			assert.strictEqual(error.code, 13);
			assert.isNull(result);
		});
	});

	//touchPromise()
	it(`${iteration++}. touchPromise() success test`, async () => {
		const { key } = oThing(1);

		try {
			const tp = await CBS.touchPromise(key, 0);

			assert.strictEqual(tp.constructor.name, 'MutationResult');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it(`${iteration++}. touchPromise() failure test`, async () => {
		const { key } = oThing(7);

		try {
			await CBS.touchPromise(key, 0);

			throw new Error('should NOT have succeeded');
		} catch(e) {
			assert.strictEqual(e.code, 13);
		}
	});
});// END REMOVE/UNLOCK/TOUCH tests


testSuite('QUERY related tests', async (iteration) => {
	//upsertDesignDocumentCallback()
	it(`${iteration++}. upsertDesignDocumentCallback() success test`, () => {
		const view = {
			doctype_thing: {
				map:`function(doc, meta) {
					if(doc.doctype === 'thing') {
						emit(meta.id, doc);
					}
				}`
			}
		};

		CBS.upsertDesignDocumentCallback('thing', view, true, (error, response) => {
			assert.isNull(error);
			assert.isUndefined(response);
		});
	});

	//upsertDesignDocumentPromise()
	it(`${iteration++}. upsertDesignDocumentPromise() success test`, async () => {
		const view = {
			doctype_thing: {
				map:`function(doc, meta) {
					if(doc.doctype === 'thing') {
						emit(meta.id, doc);
					}
				}`
			}
		};

		try {
			const uddp = await CBS.upsertDesignDocumentPromise('thing', view, true);

			assert.isNull(uddp);
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});

	it('<WAIT FOR VQ TO POPULATE>', async () => {
		await sleep(1000);
		CBS.viewQueryCallback('dev_thing', 'doctype_thing', {}, () => {});
	}).timeout(5000);

	//viewQueryCallback()
	it(`${iteration++}. viewQueryCallback() ascending success test`, async () => {
		await sleep(1000);
		return new Promise((resolve, reject) => {
			CBS.viewQueryCallback('dev_thing', 'doctype_thing', { order:'ascending' }, (error, result) => {
				try {
					assert.isNull(error);
					assert.strictEqual(result.constructor.name, 'ViewResult');
					assert.isArray(result.rows);
					assert.isNotEmpty(result.rows);
					result.rows.map((r, index) => {
						assert.strictEqual(r.constructor.name, 'ViewRow');
						if(index) {
							assert.isAbove(Number(r.key.match(/\d+/g)[0]), Number(result.rows[index-1].key.match(/\d+/g)[0]));
						}
					});
					assert.strictEqual(result.meta.constructor.name, 'ViewMetaData');
					return resolve(null);
				} catch(e) {
					return reject(e);
				}
			});
		});
	}).timeout(5000);

	it(`${iteration++}. viewQueryCallback() desending success test`, async () => {
		await sleep(1000);
		return new Promise((resolve, reject) => {
			CBS.viewQueryCallback('dev_thing', 'doctype_thing', { order:'descending' }, (error, result) => {
				try {
					assert.isNull(error);
					assert.strictEqual(result.constructor.name, 'ViewResult');
					assert.isArray(result.rows);
					assert.isNotEmpty(result.rows);
					result.rows.map((r, index) => {
						assert.strictEqual(r.constructor.name, 'ViewRow');
						if(index) {
							assert.isBelow(Number(r.key.match(/\d+/g)[0]), Number(result.rows[index-1].key.match(/\d+/g)[0]));
						}
					});
					assert.strictEqual(result.meta.constructor.name, 'ViewMetaData');
					return resolve(null);
				} catch(e) {
					return reject(e);
				}
			});
		});
	}).timeout(5000);

	//viewQueryPromise()
	it(`${iteration++}. viewQueryPromise() ascending success test`, async () => {
		await sleep(1000);
		try {
			const vqp = await CBS.viewQueryPromise('dev_thing', 'doctype_thing', { order:'ascending' });

			assert.strictEqual(vqp.constructor.name, 'ViewResult');
			assert.isArray(vqp.rows);
			assert.isNotEmpty(vqp.rows);
			vqp.rows.map((r, index) => {
				assert.strictEqual(r.constructor.name, 'ViewRow');
				if(index) {
					assert.isAbove(Number(r.key.match(/\d+/g)[0]), Number(vqp.rows[index-1].key.match(/\d+/g)[0]));
				}
			});
			assert.strictEqual(vqp.meta.constructor.name, 'ViewMetaData');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	}).timeout(5000);

	it(`${iteration++}. viewQueryPromise() desending success test`, async () => {
		await sleep(1000);
		try {
			const vqp = await CBS.viewQueryPromise('dev_thing', 'doctype_thing', { order:'descending' });

			assert.strictEqual(vqp.constructor.name, 'ViewResult');
			assert.isArray(vqp.rows);
			assert.isNotEmpty(vqp.rows);
			vqp.rows.map((r, index) => {
				assert.strictEqual(r.constructor.name, 'ViewRow');
				if(index) {
					assert.isBelow(Number(r.key.match(/\d+/g)[0]), Number(vqp.rows[index-1].key.match(/\d+/g)[0]));
				}
			});
			assert.strictEqual(vqp.meta.constructor.name, 'ViewMetaData');
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	}).timeout(5000);

	//n1qlQueryCallback()
	it(`${iteration++}. n1qlQueryCallback() create doctype index test (long wait for index load)`, async () => {
		const n1ql = 'CREATE INDEX `doctype` ON `default`(`doctype`) USING GSI;';
		return new Promise((resolve, reject) => {
			CBS.n1qlQueryCallback(n1ql, (error, result) => {
				try {
					if(error) {
						assert.strictEqual(error.constructor.name, 'IndexExistsError');
					} else {
						assert.isArray(result);
						assert.isEmpty(result);
					}
					return resolve(null);
				} catch(e) {
					return reject(e);
				}
			});
		});
	}).timeout(10000);

	it(`${iteration++}. n1qlQueryCallback() doctype pull test`, async () => {
		const n1ql = 'SELECT * FROM `default` WHERE doctype="thing"';

		CBS.n1qlQueryCallback(n1ql, (error, result) => {
			assert.isNull(error);
			assert.isArray(result);
			assert.isNotEmpty(result);
			result.map((value) => {
				assert.isObject(value);
				assert.hasAllKeys(value, ['default']);
				assert.isObject(value.default);
				assert.hasAllKeys(value.default, ['bazinga', 'foo', 'doctype']);
			});
		});
	});

	it(`${iteration++}. n1qlQueryPromise() doctype pull test`, async () => {
		const n1ql = 'SELECT * FROM `default` WHERE doctype="thing"';

		try {
			const nqp = await CBS.n1qlQueryPromise(n1ql);

			assert.isArray(nqp);
			assert.isNotEmpty(nqp);
			nqp.map((value) => {
				assert.isObject(value);
				assert.hasAllKeys(value, ['default']);
				assert.isObject(value.default);
				assert.hasAllKeys(value.default, ['bazinga', 'foo', 'doctype']);
			});
		} catch(e) {
			throw new Error(`should NOT have failed: ${e.message}`);
		}
	});
});// END QUERY related tests


testSuite('CLEANUP', async (iteration) => {
	it(`${iteration++}. remove remaining docs`, async () => {
		try {
			for(let { key } of (freshDocs().concat(oThing(5)))) {
				try {
					await CBS.removePromise(key);

					console.log(`Removed doc ${key}`);
				} catch(e) {
					console.log(`Error removing doc ${key}`);
				}
			}
		} catch(e) {
			console.log(`Error removing docs: ${e.message}`);
		}
	});

	it(`${iteration++}. remove n1ql indexes`, async () => {
		try {
			for(let { bucket, index } of [{ bucket:'default', index:'doctype' }]) {
				try {
					await CBS.n1qlQueryPromise(`DROP INDEX ${bucket}.${index};`);

					console.log(`Removed index ${bucket}.${index}`);
				} catch(e) {
					console.log(`Error removing index ${bucket}.${index}: ${e.message}`);
				}
			}
		} catch(e) {
			console.log(`Error removing indexes: ${e.message}`);
		}
	});

	it(`${iteration++}. remove view queries`, async () => {
		try {
			for(let designDocName of ['thing']) {
				try {
					await CBS.dropDesignDocumentPromise(designDocName, true);

					console.log(`Removed view query design doc: ${designDocName}`);
				} catch(e) {
					console.log(`Error removing view query design doc: ${designDocName}: ${e.message}`);
				}
			}
		} catch(e) {
			console.log(`Error removing view queries: ${e.message}`);
		}
	});
});// CLEANUP


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