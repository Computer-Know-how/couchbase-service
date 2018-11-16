/*
	DEPENDENCIES --------------------------------------------------------------------------
*/

const { assert } = require('chai');
const mockery = require('mockery');

let couchbaseService; // load manually w/mockery


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


testSuite('example tests', () => {
	it('Example test', () => {
		assert.isTrue(true);
	});
}); // END example tests
