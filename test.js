
require('dotenv').config();

const StormiDB = require('./src/StormiDB');
const AzureBlobStorage = require('./src/storage/AzureBlobStorage');

const connectionString = process.env.AZURE_BLOB_STORAGE;
const storage = new AzureBlobStorage(connectionString);

const stormiDB = new StormiDB(storage);

// Now you can use db to interact with your data

//
async function main(){

  const db = new StormiDB(storage);

  const collectionName = 'orders8';

  // Create indexes
  await db.createIndex(collectionName, 'status');
  await db.createIndex(collectionName, 'createdAt', { type: 'date', granularity: 'daily' });
  await db.createIndex(collectionName, ['status', 'createdAt'], { type: 'compound' });

  // // Add 1,000 documents
  // const statuses = ['pending', 'shipped', 'delivered', 'canceled'];
  // const now = Date.now();

  // console.log('Adding documents...');
  // const batchSize = 10; // Insert in batches of 20
  // let batch = [];
  // return;
  // for (let i = 0; i < 1000; i++) {
  //     const data = {
  //         orderId: `ORD${i.toString().padStart(4, '0')}`,
  //         status: statuses[Math.floor(Math.random() * statuses.length)],
  //         createdAt: new Date(now - Math.floor(Math.random() * 30) * 24 * 60 * 60 * 1000).toISOString(),
  //         amount: Math.floor(Math.random() * 1000) + 100,
  //     };

  //     batch.push(db.create(collectionName, data));

  //     if (batch.length === batchSize) {
  //         await Promise.all(batch);
  //         console.log(`Added ${i + 1} documents`);
  //         batch = []; // Reset the batch
  //     }
  // }

  // // Insert any remaining documents
  // if (batch.length > 0) {
  //     await Promise.all(batch);
  //     console.log(`Added remaining documents`);
  // }

  console.log('Testing queries...');

  // Test with compound index
  const query1 = {
    status: { operator: 'EQ', value: 'shipped' },
    createdAt: { operator: 'BETWEEN', value: ['2023-09-01', '2023-09-30'] },
  };
  const results1 = await db.find(collectionName, query1);
  console.log(`Query with compound index returned ${results1.length} documents`);

  // Test combining single-field indexes
  const query2 = {
    status: { operator: 'EQ', value: 'delivered' },
    amount: { operator: 'GT', value: 500 },
  };
  const results2 = await db.find(collectionName, query2);
  console.log(`Query combining indexes returned ${results2.length} documents`);

  // Test with no indexes (full scan)
  const query3 = {
    amount: { operator: 'LT', value: 300 },
    customerName: { operator: 'EQ', value: 'John Doe' },
  };
  const results3 = await db.find(collectionName, query3);
  console.log(`Query with no indexes returned ${results3.length} documents`);

  // await stormiDB.createIndex('orders', 'createdAt', {
  //   type: 'date',
  //   granularity: 'daily',
  //   createOnlyIfNotExists: true
  // });

  // const results = await stormiDB.find('orders', {
  //   createdAt: { operator: 'BETWEEN', value: ['2023-09-01', '2023-09-30'] },
  // });

  // console.log('results', results)
  // await stormiDB.createIndex('users', 'email', { unique: true, createOnlyIfNotExists: true });

  // await stormiDB.create('users', { email: 'user2@example.com', firstName: 'John', lastName: 'Doe' });

  // const youngAdults = await stormiDB.find('users', {email: 'user2@example.com'});
  // console.log(youngAdults);

  

  // return  
  // Create a unique index on a single field
// await stormiDB.createIndex('users', 'email', { unique: true, createOnlyIfNotExists: true });

// // Create a unique index on a group of fields
// await stormiDB.createIndex('users', ['firstName', 'lastName'], { unique: true, createOnlyIfNotExists: true });

// // Create a non-unique index
// await stormiDB.createIndex('users', 'age', { createOnlyIfNotExists: false });

// // This will throw an error if a user with the same email already exists
// await stormiDB.create('users', { email: 'user@example.com', firstName: 'John', lastName: 'Doe' });

// This will throw an error if a user with the same first name and last name combination already exists
// await stormiDB.create('users', { email: 'another@example.com', firstName: 'John', lastName: 'Doe' });
  
}

main();