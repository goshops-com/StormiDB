
require('dotenv').config();

const StormiDB = require('./src/StormiDB');
const AzureBlobStorage = require('./src/storage/AzureBlobStorage');

const connectionString = process.env.AZURE_BLOB_STORAGE;
const storage = new AzureBlobStorage(connectionString);

// Now you can use db to interact with your data

//
async function main(){
  try {
    const db = new StormiDB(storage);

    // Define the collection name
    const collectionName = 'users100';

    // Create indexes
    await db.createIndex(collectionName, 'email', { unique: true });
    await db.createIndex(collectionName, ['firstName', 'lastName'], { type: 'compound' });
    await db.createIndex(collectionName, 'createdAt', { type: 'date', granularity: 'daily' });

    console.log('Indexes created.');

    // Insert documents
    const userId1 = await db.create(collectionName, {
      firstName: 'John',
      lastName: 'Doe',
      email: 'john.doe@example.com',
      createdAt: new Date().toISOString(),
    });

    const userId2 = await db.create(collectionName, {
      firstName: 'Jane',
      lastName: 'Smith',
      email: 'jane.smith@example.com',
      createdAt: new Date().toISOString(),
    });

    console.log('Documents inserted:', userId1, userId2);

    // Try to insert a document with a duplicate email (should fail)
    try {
      await db.create(collectionName, {
        firstName: 'Jim',
        lastName: 'Beam',
        email: 'john.doe@example.com', // Duplicate email
        createdAt: new Date().toISOString(),
      });
    } catch (error) {
      console.error('Expected error on duplicate email:', error.message);
    }

    // Find a document by ID
    const user1 = await db.findById(collectionName, userId1);
    console.log('User 1:', user1);

    // Find documents with a query
    const usersNamedJohn = await db.find(collectionName, { firstName: 'John' });
    console.log('Users named John:', usersNamedJohn);

    // Update a document
    await db.update(collectionName, userId1, {
      firstName: 'Johnny',
      lastName: 'Doe',
      email: 'johnny.doe@example.com', // Update email
      createdAt: user1.createdAt, // Keep the original creation date
    });

    console.log('User 1 updated.');

    // Verify that the unique index has been updated
    try {
      await db.create(collectionName, {
        firstName: 'Jack',
        lastName: 'Daniels',
        email: 'johnny.doe@example.com', // Duplicate updated email
        createdAt: new Date().toISOString(),
      });
    } catch (error) {
      console.error('Expected error on duplicate updated email:', error.message);
    }

    // Delete a document
    await db.delete(collectionName, userId2);
    console.log('User 2 deleted.');

    // Count documents
    const userCount = await db.countDocuments(collectionName, {});
    console.log('Total users:', userCount);

    // Find documents using the date index
    const today = new Date();
    const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate()).toISOString();
    const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate() + 1).toISOString();

    const usersCreatedToday = await db.find(collectionName, {
      createdAt: {
        $gte: startOfDay,
        $lt: endOfDay,
      },
    });
    console.log('Users created today:', usersCreatedToday);

    const usersCreatedToday2 = await db.find(collectionName, {
      createdAt: {
        $gte: startOfDay,
        $lt: endOfDay,
      },
    }, { analyze: true });
    console.log('Analize Users created today:', usersCreatedToday2);


    const allUsers1 = await db.find(collectionName, {}, {limit:1, offset:0});
    console.log('allUsers', allUsers1);

    const allUsers2 = await db.find(collectionName, {}, {limit:1, offset:1});
    console.log('allUsers', allUsers2);

    // Clean up: drop the collection
    await db.dropCollection(collectionName);
    console.log('Collection dropped.');
  } catch (err) {
    console.error('An error occurred during testing:', err);
  }
}

main();