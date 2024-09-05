
require('dotenv').config();

const StormiDB = require('./src/StormiDB');
const AzureBlobStorage = require('./src/storage/AzureBlobStorage');

const connectionString = process.env.AZURE_BLOB_STORAGE;
const storage = new AzureBlobStorage(connectionString, {
  prefix: 'poc1'
});

const db = new StormiDB(storage);

// Now you can use db to interact with your data


async function main(){
  //const userId = await db.create('users', { name: 'Alice', age: 30 });
  //console.log(`Created user with ID: ${userId}`);

  //await db.createIndex('users', 'age');
  const youngAdults = await db.find('users', { age: { $gte: 18, $lt: 30 } });
  console.log('Young adults:', youngAdults);
}

main();