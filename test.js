
require('dotenv').config();

const StormiDB = require('./src/StormiDB');
const AzureBlobStorage = require('./src/storage/AzureBlobStorage');

const connectionString = process.env.AZURE_BLOB_STORAGE;
const storage = new AzureBlobStorage(connectionString, {
  prefix: 'poc6'
});

const stormiDB = new StormiDB(storage);

// Now you can use db to interact with your data


async function main(){

  // await stormiDB.createIndex('users', 'email', { unique: true, createOnlyIfNotExists: true });

  // await stormiDB.create('users', { email: 'user2@example.com', firstName: 'John', lastName: 'Doe' });

  const youngAdults = await stormiDB.find('users', {email: 'user2@example.com'});
  console.log(youngAdults);

  

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