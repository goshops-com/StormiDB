require('dotenv').config();
const AzureBlobStorage = require('../src/storage/AzureBlobStorage');

const connectionString = process.env.AZURE_BLOB_STORAGE;
const storage = new AzureBlobStorage(connectionString);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runTest() {
  const collectionName = 'test_tag_query6';

  try {
    
    console.log('Creating index...');
    await storage.createIndex(collectionName, ['age', 'city']);
    

    console.log('Inserting test data...');
    const testData = [
        { name: 'Alice', age: 30, city: 'New York', profession: 'Engineer' },
        { name: 'Bob', age: 25, city: 'Los Angeles', profession: 'Designer' },
        { name: 'Charlie', age: 35, city: 'Chicago', profession: 'Manager' },
        { name: 'David', age: 30, city: 'New York', profession: 'Engineer' },
      ];      

    for (const data of testData) {
      await storage.create(collectionName, data);
      await sleep(5000); // Wait a bit between inserts
    }

    await sleep(20000);
    // return;

    console.log('Running queries...');
    
    // Query 1: Single tag
    console.log('\nQuery 1: age = 30');
    let result = await storage.find(collectionName, { age: 30 });
    console.log('Result:', result);

    // Query 2: Multiple tags
    console.log('\nQuery 2: age = 30 AND city = "New York"');
    result = await storage.find(collectionName, { age: 30, city: 'New York' });
    console.log('Result:', result);

    // Query 3: Non-existent tag value
    console.log('\nQuery 3: age = 40');
    result = await storage.find(collectionName, { age: 40 });
    console.log('Result:', result);

    // Query 4: Non-indexed field
    console.log('\nQuery 4: profession = "Engineer"');
    result = await storage.find(collectionName, { profession: 'Engineer' });
    console.log('Result:', result);

    // Query 5: Empty query (pagination)
    console.log('\nQuery 5: Empty query (first 2 documents)');
    result = await storage.find(collectionName, {}, { limit: 2 });
    console.log('Result:', result);

    // Query 6: Empty query (pagination with offset)
    console.log('\nQuery 6: Empty query (last 2 documents)');
    result = await storage.find(collectionName, {}, { offset: 2, limit: 2 });
    console.log('Result:', result);

    console.log('Running complex queries...');
    
    /// Test greater than
    console.log('\nQuery: age > 30');
    result = await storage.find(collectionName, { age: { $gt: 30 } });
    console.log('Result:', result);

    // Test between
    console.log('\nQuery: 25 < age < 35');
    result = await storage.find(collectionName, { age: { $between: [25, 35] } });
    console.log('Result:', result);

    // Test equality
    console.log('\nQuery: city = "New York"');
    result = await storage.find(collectionName, { city: 'New York' });
    console.log('Result:', result);

    // Test complex query (indexed + non-indexed fields)
    console.log('\nQuery: age >= 30 AND score > 80');
    result = await storage.find(collectionName, { 
      age: { $gte: 30 },
      score: { $gt: 80 }
    });
    console.log('Result:', result);

  } catch (error) {
    console.error('An error occurred:', error);
  }
}

runTest();