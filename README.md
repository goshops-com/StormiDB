# StormiDB

StormiDB is a lightweight, document-oriented database library that uses cloud object storage (Azure Blob Storage and S3-compatible) as its backend. It's designed for simplicity, scalability, and resilience.

## Why StormiDB?

StormiDB isn't trying to replace MongoDB or other full-featured NoSQL databases. Instead, it offers a simpler alternative for projects that:

- Need a document store with basic querying capabilities
- Want to leverage the scalability and durability of cloud object storage
- Prefer a simpler architecture with fewer moving parts

By relying solely on object storage, StormiDB makes your data architecture simpler and inherits the scalability and resilience characteristics of major cloud providers.

## Installation

```bash
npm install stormidb
```

## Quick Start

```javascript
const StormiDB = require('stormidb');

// For Azure Blob Storage
const db = new StormiDB.AzureAdapter('YOUR_AZURE_CONNECTION_STRING');

// For S3-compatible storage
// const db = new StormiDB.S3Adapter({
//   endpoint: 'YOUR_S3_ENDPOINT',
//   credentials: {
//     accessKeyId: 'YOUR_ACCESS_KEY',
//     secretAccessKey: 'YOUR_SECRET_KEY'
//   }
// });

// Create a document
const userId = await db.create('users', { name: 'Alice', age: 30 });
console.log(`Created user with ID: ${userId}`);

// Find documents
const users = await db.find('users', { age: { $gt: 25 } });
console.log('Users over 25:', users);

// Update a document
await db.update('users', userId, { name: 'Alice', age: 31 });

// Delete a document
await db.delete('users', userId);
```

## API Reference

### Constructor

- `new StormiDB.AzureAdapter(connectionString)`
- `new StormiDB.S3Adapter(config)`

### Methods

- `create(collection, document, [id])`: Create a new document. Returns the document ID.
- `find(collection, query, [options])`: Find documents matching the query.
- `findOne(collection, query)`: Find a single document matching the query.
- `update(collection, id, document)`: Update a document by ID.
- `delete(collection, id)`: Delete a document by ID.
- `createIndex(collection, field)`: Create an index on a field.

### Query Operators

StormiDB supports basic comparison operators:

- `$eq`: Equal to
- `$gt`: Greater than
- `$lt`: Less than
- `$gte`: Greater than or equal to
- `$lte`: Less than or equal to

### Options

The `find` method accepts an options object:

- `limit`: Maximum number of results to return
- `offset`: Number of results to skip

## Examples

### Creating and querying documents

```javascript
// Create some users
await db.create('users', { name: 'Bob', age: 25 });
await db.create('users', { name: 'Charlie', age: 35 });

// Find users over 30
const adultsOver30 = await db.find('users', { age: { $gt: 30 } });
console.log('Adults over 30:', adultsOver30);

// Find the first user named Bob
const bob = await db.findOne('users', { name: 'Bob' });
console.log('Bob:', bob);
```

### Using indexes for better performance

```javascript
// Create an index on the 'age' field
await db.createIndex('users', 'age');

// This query will now use the index
const youngAdults = await db.find('users', { age: { $gte: 18, $lt: 30 } });
console.log('Young adults:', youngAdults);
```

### Pagination

```javascript
// Get the first 10 users
const firstPage = await db.find('users', {}, { limit: 10 });

// Get the next 10 users
const secondPage = await db.find('users', {}, { limit: 10, offset: 10 });
```

## Limitations

- StormiDB is not a full replacement for traditional databases. It's designed for simplicity and may not be suitable for complex querying needs or high-concurrency scenarios.
- Performance may vary depending on your object storage configuration and network conditions.
- Transactions are not supported.

## Contributing

We welcome contributions! Please see our contributing guidelines for more details.

## License

StormiDB is released under the MIT License.