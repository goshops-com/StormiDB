# StormiDB

StormiDB is a flexible and efficient NoSQL database library that uses Azure Blob Storage as its backend. It provides a simple interface for performing CRUD operations, creating indexes, and querying data.

## Features

- CRUD operations (Create, Read, Update, Delete)
- Indexing support (including compound, date, and default indexes)
- Querying with support for various operators
- Automatic ID generation using ULID
- Concurrency control with optimistic locking
- Pagination support

## Installation

```bash
npm install stormidb
```

## Usage

### Initializing StormiDB

```javascript
const { StormiDB, AzureBlobStorage } = require('stormidb');

const connectionString = 'your_azure_storage_connection_string';
const storage = new AzureBlobStorage(connectionString);
const db = new StormiDB(storage);
```

### List collections

```javascript
const stormi = new StormiDB(storage);
const collections = await stormi.getCollections();
console.log(collections); // This will print an array of collection names
```

### Creating a Document

```javascript
const collection = 'users';
const userData = {
  name: 'John Doe',
  email: 'john@example.com',
  age: 30
};

const userId = await db.create(collection, userData);
console.log('Created user with ID:', userId);
```

### Reading a Document by ID

```javascript
const user = await db.findById('users', userId);
console.log('Found user:', user);
```

### Reading a Document

```javascript
const user = await db.findOne('users', { email: 'john@example.com' });
console.log('Found user:', user);
```

### Updating a Document

```javascript
const updatedData = { age: 31 };
await db.update('users', userId, updatedData);
console.log('Updated user age');
```

### Deleting a Document

```javascript
await db.delete('users', userId);
console.log('Deleted user');
```

### Creating an Index

```javascript
await db.createIndex('users', 'email', { unique: true });
console.log('Created unique index on email field');
```

### Querying Documents

```javascript
const query = { age: { $gte: 25, $lt: 35 } };
const users = await db.find('users', query);
console.log('Users aged 25-34:', users);
```

### Creating a Compound Index

```javascript
await db.createIndex('users', ['name', 'email'], { type: 'compound' });
console.log('Created compound index on name and email fields');
```

### Creating a Date Index

```javascript
await db.createIndex('events', 'eventDate', { type: 'date', granularity: 'daily' });
console.log('Created date index on eventDate field');
```

### Querying with a Date Index

```javascript
const startDate = new Date('2023-01-01');
const endDate = new Date('2023-12-31');
const events = await db.find('events', {
  eventDate: { $between: [startDate, endDate] }
});
console.log('Events in 2023:', events);
```

### Pagination

StormiDB supports pagination through the `limit` and `offset` options in the `find` method:

```javascript
const pageSize = 10;
const page = 1;

const users = await db.find('users', {}, {
  limit: pageSize,
  offset: (page - 1) * pageSize
});

console.log(`Users on page ${page}:`, users);
```

To implement pagination in your application:

1. Decide on a page size (e.g., 10 items per page).
2. Calculate the offset based on the current page number: `offset = (pageNumber - 1) * pageSize`.
3. Use these values in the `find` method options.
4. To get the total count of items, you may need to perform a separate query without `limit` and `offset`.

## Index Types and When to Use Them

StormiDB supports three types of indexes:

1. **Default Index**: Used for general-purpose indexing on a single field.
   - When to use: For fields that you frequently query with equality or range conditions.
   - Example: `await db.createIndex('users', 'age');`

2. **Compound Index**: Used for indexing multiple fields together.
   - When to use: When you often query on a combination of fields or need to support sorting on multiple fields.
   - Example: `await db.createIndex('users', ['lastName', 'firstName'], { type: 'compound' });`

3. **Date Index**: Optimized for date-based queries.
   - When to use: For fields containing dates that you frequently use in range queries or for time-based data analysis.
   - Example: `await db.createIndex('events', 'eventDate', { type: 'date', granularity: 'daily' });`

Choose the appropriate index type based on your query patterns:

- If you frequently query on a single field, use a default index.
- If you often query on combinations of fields, use a compound index.
- If you perform many date-range queries, use a date index.

Remember that indexes improve query performance but can slow down write operations. Balance the number of indexes with your read/write patterns.

## How StormiDB Works Internally

StormiDB uses Azure Blob Storage as its backend, with the following key components:

1. **Collections**: Each collection is represented as a container in Azure Blob Storage.

2. **Documents**: Each document is stored as a JSON blob within its collection's container.

3. **Indexes**: Indexes are stored as separate blobs in a special `__indexes` container.

4. **CRUD Operations**:
   - Create: Generates a new ULID for the document (if not provided) and stores it as a JSON blob.
   - Read: Retrieves the JSON blob and parses it into a JavaScript object.
   - Update: Replaces the existing JSON blob with the updated data.
   - Delete: Removes the JSON blob from the container.

5. **Querying**:
   - The query parser converts the query object into a structured format.
   - The system attempts to use the best available index for the query.
   - If no suitable index is found, it performs a full collection scan.

6. **Indexing**:
   - Default indexes store a mapping of indexed field values to document IDs.
   - Compound indexes combine multiple fields into a single index entry.
   - Date indexes use a special structure optimized for range queries on dates.

7. **Concurrency Control**: Uses ETags for optimistic locking to handle concurrent modifications.

8. **Pagination**: Implemented using the `limit` and `offset` options in the query process.

This architecture allows StormiDB to provide a document database-like interface while leveraging the scalability and durability of Azure Blob Storage.

## API Reference

### StormiDB Class

- `constructor(storage)`: Creates a new StormiDB instance with the given storage backend.
- `create(collection, data, id = null)`: Creates a new document in the specified collection.
- `findById(collection, id)`: Retrieves a document by its ID.
- `find(collection, query, options = {})`: Finds documents in the collection that match the query.
- `findOne(collection, query)`: Finds the first document that matches the query.
- `update(collection, id, data)`: Updates a document with the specified ID.
- `delete(collection, id)`: Deletes a document with the specified ID.
- `createIndex(collection, field, options = {})`: Creates an index on the specified field(s).
- `dropCollection(collection)`: Drops the entire collection.

### AzureBlobStorage Class

- `constructor(connectionString, options = {})`: Creates a new AzureBlobStorage instance.
- `create(collection, id, data)`: Creates a new document in the specified collection.
- `read(collection, id)`: Reads a document with the specified ID.
- `update(collection, id, data)`: Updates a document with the specified ID.
- `delete(collection, id)`: Deletes a document with the specified ID.
- `find(collection, query, options = {})`: Finds documents in the collection that match the query.
- `createIndex(collection, fields, options = {})`: Creates an index on the specified field(s).
- `dropCollection(collection)`: Drops the entire collection.

## Query Operators

StormiDB supports the following query operators:

- `$eq`: Equality
- `$ne`: Not equal
- `$gt`: Greater than
- `$gte`: Greater than or equal to
- `$lt`: Less than
- `$lte`: Less than or equal to
- `$in`: In array
- `$nin`: Not in array
- `$and`: Logical AND
- `$or`: Logical OR
- `$not`: Logical NOT
- `$exists`: Field exists
- `$type`: Field is of specified type
- `$regex`: Regular expression match
- `$between`: Between two values (inclusive)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.