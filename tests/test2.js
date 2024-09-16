require('dotenv').config();

const AzureBlobStorage = require('../src/storage/AzureBlobStorage');

const connectionString = process.env.AZURE_BLOB_STORAGE;
const storage = new AzureBlobStorage(connectionString);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function stressTest() {
  try {
    const collectionName = 'stress_test_users';

    // Clean up: drop the collection before starting
    // await storage.dropCollection(collectionName);

    // Create indexes
    await storage.createIndex(collectionName, 'email', { unique: true });
    await storage.createIndex(collectionName, 'age'); // Index on numeric field
    await storage.createIndex(collectionName, 'createdAt'); // Index on date field

    console.log('Indexes created.');

    // Prepare to insert 10,000 users in batches of 50
    const totalDocuments = 10000;
    const batchSize = 50;
    let createdDocuments = 0;

    console.time('Insertion Time');

    // for (let i = 0; i < totalDocuments / batchSize; i++) {
    //   const batch = [];

    //   for (let j = 0; j < batchSize; j++) {
    //     const userIndex = i * batchSize + j;
    //     const user = {
    //       firstName: `User${userIndex}`,
    //       lastName: `LastName${userIndex}`,
    //       email: `user${userIndex}@example.com`,
    //       age: Math.floor(Math.random() * 60) + 20, // Age between 20 and 80
    //       createdAt: new Date(
    //         2022,
    //         Math.floor(Math.random() * 12), // Random month
    //         Math.floor(Math.random() * 28) + 1 // Random day
    //       ).toISOString(),
    //     };
    //     batch.push(user);
    //   }

    //   // Insert the batch
    //   const promises = batch.map((user) => storage.create(collectionName, user));
    //   await Promise.all(promises);

    //   createdDocuments += batchSize;
    //   console.log(`Inserted ${createdDocuments} documents`);

    //   // Optional: Sleep for a short period to avoid overwhelming the system
    //   await sleep(100); // Sleep for 100ms between batches
    // }

    console.timeEnd('Insertion Time');
    console.log(`Finished inserting ${totalDocuments} documents.`);

    // Now, use the filter and paginate the results
    const ageFilter = { age: { $gte: 30 } }; // Example: filter for users with age >= 30
    const pageSize = 100;
    let currentPage = 0;
    let moreResults = true;
    console.time('Query Time');

    while (moreResults) {
      const offset = currentPage * pageSize;
      const users = await storage.find(collectionName, ageFilter, {
        limit: pageSize,
        offset,
      });

      if (users.length === 0) {
        moreResults = false; // No more results
      } else {
        console.log(`Page ${currentPage + 1}: Retrieved ${users.length} users`);
        currentPage++;
      }
    }

    console.timeEnd('Query Time');

    // Clean up: drop the collection
    await storage.dropCollection(collectionName);
    console.log('Collection dropped.');
  } catch (err) {
    console.error('An error occurred during stress testing:', err);
  }
}

stressTest();
