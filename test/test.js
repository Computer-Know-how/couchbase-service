/*
	DEPENDENCIES --------------------------------------------------------------------------
*/

const { assert } = require('chai');

let index = null;// load manually w/mockery


/*
	DESCRIBE: SETUP/TEARDOWN WRAPPER --------------------------------------------------------------------------
*/

function testSuite(testName, tests) {
	describe(testName, (iteration=1) => {
		//SETUP
		before(() => {
			index = require('../lib/index.js');
		});

		beforeEach(() => {});

		//TESTS
		tests(iteration);

		//TEARDOWN
		afterEach(() => {});

		after(() => {
			index = null;
		});
	});
}


/*
	TESTS -------------------------------------------------------------------------------------------------
*/

testSuite('index composition test', async (iteration) => {
	it(`${iteration++}. composition test`, () => {
		assert.isObject(index);
		assert.hasAllKeys(index, [
			'CouchbaseService',
			'mapErrorCodes',
			'isPopulatedObject',
			'isObjectWithProperty',
			'valueFromResult',
			'contentToValue']
		);

		for(let attribute in index) {
			assert.isFunction(index[attribute]);
		}
	});
});// END index composition test


//NOTE: CouchbaseService is tested in depth with the live Couchbase connection suite


testSuite('mapErrorCodes() tests', (iteration) => {
	it(`${iteration++}. generic error test`, () => {
		const dnfe = new Error('test');

		const mec = index.mapErrorCodes(dnfe);

		assert.strictEqual(mec.constructor.name, 'Error');
		assert.notProperty(mec, 'code');
		assert.strictEqual(mec.message, 'test');
	});

	it(`${iteration++}. DocumentNotFoundError tests`, () => {
		const cause = 'manually created';
		const context = 'test';
		const dnfe = new DocumentNotFoundError(cause, context);

		const mec = index.mapErrorCodes(dnfe);

		assert.hasAllKeys(mec, ['name', 'code', 'cause', 'context']);
		assert.strictEqual(mec.name, 'DocumentNotFoundError');
		assert.strictEqual(mec.code, 13);
		assert.strictEqual(mec.cause, cause);
		assert.strictEqual(mec.context, context);
	});

	it(`${iteration++}. DocumentExistsError tests`, () => {
		const cause = 'manually created';
		const context = 'test';
		const dnfe = new DocumentExistsError(cause, context);

		const mec = index.mapErrorCodes(dnfe);

		assert.hasAllKeys(mec, ['name', 'code', 'cause', 'context']);
		assert.strictEqual(mec.name, 'DocumentExistsError');
		assert.strictEqual(mec.code, 12);
		assert.strictEqual(mec.cause, cause);
		assert.strictEqual(mec.context, context);
	});

	it(`${iteration++}. TemporaryFailureError tests`, () => {
		const cause = 'manually created';
		const context = 'test';
		const dnfe = new TemporaryFailureError(cause, context);

		const mec = index.mapErrorCodes(dnfe);

		assert.hasAllKeys(mec, ['name', 'code', 'cause', 'context']);
		assert.strictEqual(mec.name, 'TemporaryFailureError');
		assert.strictEqual(mec.code, 11);
		assert.strictEqual(mec.cause, cause);
		assert.strictEqual(mec.context, context);
	});

	it(`${iteration++}. DocumentLockedError tests`, () => {
		const cause = 'manually created';
		const context = 'test';
		const dnfe = new DocumentLockedError(cause, context);

		const mec = index.mapErrorCodes(dnfe);

		assert.hasAllKeys(mec, ['name', 'code', 'cause', 'context']);
		assert.strictEqual(mec.name, 'DocumentLockedError');
		assert.strictEqual(mec.code, 11);
		assert.strictEqual(mec.cause, cause);
		assert.strictEqual(mec.context, context);
	});

	it(`${iteration++}. TimeoutError tests`, () => {
		const cause = 'manually created';
		const context = 'test';
		const dnfe = new TimeoutError(cause, context);

		const mec = index.mapErrorCodes(dnfe);

		assert.hasAllKeys(mec, ['name', 'code', 'cause', 'context']);
		assert.strictEqual(mec.name, 'TimeoutError');
		assert.strictEqual(mec.code, 23);
		assert.strictEqual(mec.cause, cause);
		assert.strictEqual(mec.context, context);
	});
});//END mapErrorCodes() tests


testSuite('isPopulatedObject() tests', (iteration) => {
	it(`${iteration++}. failure tests`, () => {
		for(let badData of ['bazinga', 42, true, ['bazinga'], {}]) {
			assert.isFalse(index.isPopulatedObject(badData));
		}
	});

	it(`${iteration++}. success test`, () => {
		assert.isTrue(index.isPopulatedObject({ bazinga: 'bazinga' }));
	});
});//END isPopulatedObject() tests


testSuite('isObjectWithProperty() tests', (iteration) => {
	it(`${iteration++}. failure tests`, () => {
		for(let badData of ['bazinga', 42, true, ['bazinga'], {}]) {
			assert.isFalse(index.isObjectWithProperty(badData));
		}

		//wrong key
		assert.isFalse(index.isObjectWithProperty({ bazinga:'bazinga' }, 'foo'));
	});

	it(`${iteration++}. success test`, () => {
		assert.isTrue(index.isObjectWithProperty({ bazinga: 'bazinga' }, 'bazinga'));
	});
});//END isObjectWithProperty() tests


testSuite('valueFromResult() tests', (iteration) => {
	it(`${iteration++}. direct return tests`, () => {
		for(let simpleValue of ['bazinga', 42, true]) {
			assert.strictEqual(simpleValue, index.valueFromResult(simpleValue));
		}

		for(let nonPopulatedObject of [['bazinga'], {}]) {
			assert.deepEqual(nonPopulatedObject, index.valueFromResult(nonPopulatedObject));
		}
	});

	it(`${iteration++}. populate object adjustments test`, () => {
		const oBazinga = { bazinga: 'bazinga' };
		assert.deepEqual(oBazinga, index.valueFromResult(oBazinga));

		const oValue = { value: 'bazinga' };
		assert.strictEqual(oValue.value, index.valueFromResult(oValue));

		const oContent = { content: 'bazinga' };
		assert.strictEqual(oContent.content, index.valueFromResult(oContent));

		const oBothContentNValue = { content: 'bazinga', value: 'foo' };
		assert.strictEqual(oBothContentNValue.content, index.valueFromResult(oBothContentNValue));
	});
});//END valueFromResult() tests


testSuite('contentToValue() tests', (iteration) => {
	it(`${iteration++}. direct return tests`, () => {
		for(let simpleValue of ['bazinga', 42, true]) {
			assert.strictEqual(simpleValue, index.contentToValue(simpleValue));
		}

		for(let nonPopulatedObject of [['bazinga'], {}]) {
			assert.deepEqual(nonPopulatedObject, index.contentToValue(nonPopulatedObject));
		}
	});

	it(`${iteration++}. content-to-value adjustments test`, () => {
		const oContent = { content: 'bazinga', cas:{ cas:'1234' } };
		assert.deepEqual({ value: 'bazinga', cas:{ cas:'1234' } }, index.contentToValue(oContent));

		const oBothContentNValue = { content: 'bazinga', value: 'foo', cas:{ cas:'1234' } };
		assert.strictEqual(oBothContentNValue, index.contentToValue(oBothContentNValue));
	});
});//END contentToValue() tests


/*
	HELPER FUNCTIONS -------------------------------------------------------------------------------------------------
*/

class CouchbaseError extends Error {
	constructor(message, cause, context) {
		super(message);
		this.name = this.constructor.name;
		this.cause = cause;
		this.context = context;
	}
}

class DocumentNotFoundError extends CouchbaseError {
	constructor(cause, context) {
		super('document not found', cause, context);
	}
}

class DocumentExistsError extends CouchbaseError {
	constructor(cause, context) {
		super('document exists', cause, context);
	}
}

class TemporaryFailureError extends CouchbaseError {
	constructor(cause, context) {
		super('temporary failure', cause, context);
	}
}

class DocumentLockedError extends CouchbaseError {
	constructor(cause, context) {
		super('document locked', cause, context);
	}
}

class TimeoutError extends CouchbaseError {
	constructor(cause, context) {
		super('timeout', cause, context);
	}
}