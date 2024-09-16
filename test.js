require('dotenv').config();

const AzureBlobStorage = require('./src/storage/AzureBlobStorage');

const connectionString = process.env.AZURE_BLOB_STORAGE;
const storage = new AzureBlobStorage(connectionString);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  try {
    const collectionName = 'users';

    // Clean up: drop the collection before starting
    // await storage.dropCollection(collectionName);
    await sleep(5 * 1000);

    // Create indexes
    await storage.createIndex(collectionName, 'email', { unique: true });
    await storage.createIndex(collectionName, 'age'); // Index on numeric field
    await storage.createIndex(collectionName, 'createdAt'); // Index on date field
    await storage.createIndex(collectionName, 'firstName'); // Index on string field

    console.log('Indexes created.');

    // Insert documents with delays
    await sleep(5000); // 5-second delay
    const userId1 = await storage.create(collectionName, {
      firstName: 'John_Doe', // Includes underscore
      lastName: 'Doe',
      email: 'john.doe@example.com',
      age: 30,
      createdAt: new Date('2022-01-15T10:00:00Z'), // Specific date
    });

    await sleep(5000); // 5-second delay
    const userId2 = await storage.create(collectionName, {
      firstName: 'Jane',
      lastName: 'Smith',
      email: 'jane.smith@example.com',
      age: 25,
      createdAt: new Date('2022-02-20T12:30:00Z'),
    });

    await sleep(5000); // 5-second delay
    const userId3 = await storage.create(collectionName, {
      firstName: 'Alice',
      lastName: 'Johnson',
      email: 'alice.johnson@example.com',
      age: 28,
      createdAt: new Date('2022-03-10T09:15:00Z'),
    });

    console.log('Documents inserted:', userId1, userId2, userId3);

    // Try to insert a document with a duplicate email (should fail)
    await sleep(5000); // 5-second delay
    try {
      await storage.create(collectionName, {
        firstName: 'Jim',
        lastName: 'Beam',
        email: 'john.doe@example.com', // Duplicate email
        age: 35,
        createdAt: new Date(),
      });
    } catch (error) {
      console.error('Expected error on duplicate email:', error.message);
    }

    // Find a document by ID
    const user1 = await storage.read(collectionName, userId1);
    console.log('User 1:', user1);

    // Find documents with a query: firstName equals 'John_Doe'
    const usersNamedJohnDoe = await storage.find(collectionName, { firstName: 'John_Doe' });
    console.log('Users named John_Doe:', usersNamedJohnDoe);

    // Find users older than 27
    const usersOlderThan27 = await storage.find(collectionName, { age: { $gt: 27 } });
    console.log('Users older than 27:', usersOlderThan27);

    // Find users created between specific dates
    const startDate = new Date('2022-02-01T00:00:00Z').toISOString();
    const endDate = new Date('2022-03-01T00:00:00Z').toISOString();

    const usersCreatedInFeb = await storage.find(collectionName, {
      createdAt: {
        $gte: startDate,
        $lt: endDate,
      },
    });
    console.log('Users created in February:', usersCreatedInFeb);

    // Update a document
    await sleep(5000); // 5-second delay
    await storage.update(collectionName, userId1, {
      firstName: 'Johnny',
      lastName: 'Doe',
      email: 'johnny.doe@example.com', // Update email
      age: 31,
      createdAt: user1.createdAt, // Keep the original creation date
    });

    console.log('User 1 updated.');

    // Verify that the unique index has been updated
    await sleep(5000); // 5-second delay
    try {
      await storage.create(collectionName, {
        firstName: 'Jack',
        lastName: 'Daniels',
        email: 'johnny.doe@example.com', // Duplicate updated email
        age: 40,
        createdAt: new Date(),
      });
    } catch (error) {
      console.error('Expected error on duplicate updated email:', error.message);
    }

    // Delete a document
    await sleep(5000); // 5-second delay
    await storage.delete(collectionName, userId2);
    console.log('User 2 deleted.');

    // Count documents
    const userCount = await storage.countDocuments(collectionName, {});
    console.log('Total users:', userCount);

    // Find all users (should be userId1 and userId3)
    const allUsers = await storage.find(collectionName, {});
    console.log('All users:', allUsers);

    // Clean up: drop the collection
    await sleep(5000); // 5-second delay
    await storage.dropCollection(collectionName);
    console.log('Collection dropped.');
  } catch (err) {
    console.error('An error occurred during testing:', err);
  }
}

main();
