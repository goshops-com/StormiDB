// test/compound-index.test.js

require('dotenv').config();
const AzureBlobStorage = require('../src/storage/AzureBlobStorage');

const connectionString = process.env.AZURE_BLOB_STORAGE;
const storage = new AzureBlobStorage(connectionString);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runQuery(collectionName, query, description) {
  console.log(`\n${description}`);
  console.log('Query:', JSON.stringify(query));
  const startTime = Date.now();
  const result = await storage.find(collectionName, query);
  const endTime = Date.now();
  console.log('Result:', result);
  console.log(`Query time: ${endTime - startTime}ms`);
  return result;
}

async function main() {
  try {
    const collectionName = 'users104';

    // Clean up: drop the collection before starting
    console.log('Dropping collection...');
    // await storage.dropCollection(collectionName);
    await sleep(5000);

    // Insert test data
    console.log('Inserting test data...');
    const testData = [
      { name: 'Alice', age: 30, city: 'New York', profession: 'Engineer' },
      { name: 'Bob', age: 25, city: 'Los Angeles', profession: 'Designer' },
      { name: 'Charlie', age: 35, city: 'Chicago', profession: 'Manager' },
      { name: 'David', age: 30, city: 'New York', profession: 'Engineer' },
      { name: 'Eve', age: 28, city: 'Los Angeles', profession: 'Designer' }
    ];

    for (const data of testData) {
      await storage.create(collectionName, data);
      await sleep(5000);
    }

    // Test queries without indexes
    console.log('\n--- Queries without indexes ---');
    await runQuery(collectionName, { age: 30 }, 'Query without index: age = 30');
    await runQuery(collectionName, { city: 'New York' }, 'Query without index: city = New York');
    await runQuery(collectionName, { age: 30, city: 'New York' }, 'Query without index: age = 30 AND city = New York');

    // Create single indexes
    console.log('\nCreating single indexes...');
    await storage.createIndex(collectionName, 'age');
    await sleep(5000);
    await storage.createIndex(collectionName, 'city');
    await sleep(5000);

    // Test queries with single indexes
    console.log('\n--- Queries with single indexes ---');
    await runQuery(collectionName, { age: 30 }, 'Query with single index: age = 30');
    await runQuery(collectionName, { city: 'New York' }, 'Query with single index: city = New York');
    await runQuery(collectionName, { age: 30, city: 'New York' }, 'Query with single indexes: age = 30 AND city = New York');

    // Create compound index
    console.log('\nCreating compound index...');
    await storage.createIndex(collectionName, ['age', 'city']);
    await sleep(5000);

    // Test queries using the compound index
    console.log('\n--- Queries with compound index ---');
    await runQuery(collectionName, { age: 30, city: 'New York' }, 'Query with compound index: age = 30 AND city = New York');
    await runQuery(collectionName, { age: 30 }, 'Query with compound index (partial): age = 30');
    await runQuery(collectionName, { city: 'New York' }, 'Query with compound index (partial): city = New York');

    // Test query with non-indexed field
    console.log('\n--- Query with non-indexed field ---');
    await runQuery(collectionName, { profession: 'Engineer' }, 'Query with non-indexed field: profession = Engineer');

    // Test query with mixed indexed and non-indexed fields
    console.log('\n--- Query with mixed indexed and non-indexed fields ---');
    await runQuery(collectionName, { age: 30, profession: 'Engineer' }, 'Query with mixed fields: age = 30 AND profession = Engineer');

    console.log('\nTest completed successfully!');
  } catch (error) {
    console.error('An error occurred:', error);
  }
}

main();