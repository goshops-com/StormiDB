const { BlobServiceClient } = require('@azure/storage-blob');
const { parseQuery, evaluateCondition } = require('../query/QueryParser');
const { ulid } = require('ulid');

class AzureBlobStorage {
  constructor(connectionString, options = {}) {
    this.prefix = options.prefix || '';
    this.blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    const indexContainerName = this.sanitizeContainerName(`__indexes`);
    this.indexContainer = this.blobServiceClient.getContainerClient(indexContainerName);
    this.indexContainer.createIfNotExists();
    this.uniqueConstraints = {};
  }

  sanitizeContainerName(name) {
    name = `${this.prefix}${name}`;
    let sanitized = name.toLowerCase().replace(/[^a-z0-9-]/g, '');
    sanitized = sanitized.replace(/-+/g, '-');
    sanitized = sanitized.replace(/^-|-$/g, '');
    sanitized = sanitized.substring(0, 63);
    if (sanitized.length < 3) {
      sanitized = sanitized.padEnd(3, 'a');
    }
    return sanitized;
  }

  sanitizeBlobName(name) {
    return name.replace(/[^a-zA-Z0-9-_.]/g, '-');
  }

  async getContainerClient(collection) {
    const containerName = this.sanitizeContainerName(collection);
    const containerClient = this.blobServiceClient.getContainerClient(containerName);
    await containerClient.createIfNotExists();
    return containerClient;
  }

  async create(collection, data, existingId = undefined) {
    const id = existingId || ulid(); // Ensure ulid() is available
    data.id = id;

    // Check unique constraints
    await this.checkUniqueConstraints(collection, data);

    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);
    await blobClient.upload(JSON.stringify(data), JSON.stringify(data).length);
    await this.updateIndexes(collection, id, data);
    return id;
  }

  async checkUniqueConstraints(collection, data) {
    const constraints = this.uniqueConstraints[collection] || [];
    for (const fields of constraints) {
      const indexName = Array.isArray(fields) ? fields.join('_') : fields;
      const indexKey = `${collection}_${indexName}`;
      const indexData = await this.getIndex(indexKey);
      if (indexData && indexData.unique) {
        const value = this.getIndexValue(data, fields);
        if (value !== undefined && indexData.index[value] && indexData.index[value].length > 0) {
          throw new Error(`Unique constraint violation for fields ${indexName} with value ${value}`);
        }
      }
    }
  }

  async read(collection, id) {
    try {
      const containerClient = await this.getContainerClient(collection);
      const blobClient = containerClient.getBlockBlobClient(id);
      
      // Check if the blob exists
      const exists = await blobClient.exists();
      if (!exists) {
        return null;
      }
      
      const downloadResponse = await blobClient.download();
      const downloaded = await streamToBuffer(downloadResponse.readableStreamBody);
      return JSON.parse(downloaded.toString());
    } catch (error) {
      // If there's an error (e.g., the blob doesn't exist), return null
      console.error('Error reading blob:', error);
      return null;
    }
  }

  async update(collection, id, data) {
    const oldData = await this.read(collection, id);
    await this.checkUniqueConstraints(collection, data);
    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);
    data.id = id;
    await blobClient.upload(JSON.stringify(data), JSON.stringify(data).length);
    await this.updateIndexes(collection, id, data, oldData);
  }

  async delete(collection, id) {
    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);
    const oldData = await this.read(collection, id);
    await blobClient.delete();
    await this.removeFromIndexes(collection, id, oldData);
  }

  async countDocuments(collection, query) {
    const structuredQuery = parseQuery(query);
  
    // Attempt to find the best index that can satisfy the query
    const bestIndex = await this.findBestIndex(collection, structuredQuery);
  
    let candidateIds;
  
    if (bestIndex) {
      const { indexKey, indexFields, type } = bestIndex;
  
      if (type === 'compound' && indexFields.every(field => field in structuredQuery)) {
        // Utilize a compound index if it fully covers all query fields
        candidateIds = await this.lookupCompoundIndex(collection, indexKey, structuredQuery);
      } else {
        // Attempt to combine single-field indexes for partial query coverage
        const candidateIdSets = [];
        for (const field of Object.keys(structuredQuery)) {
          const ids = await this.lookupIndexIds(collection, field, structuredQuery[field]);
          if (ids) {
            candidateIdSets.push(new Set(ids));
          } else {
            // If any field lacks an index, fallback to a full collection scan
            candidateIds = null;
            break;
          }
        }
  
        if (candidateIdSets.length > 0) {
          // Intersect the sets of candidate IDs to find common documents matching all indexed fields
          candidateIds = this.intersectSets(candidateIdSets);
        }
      }
    }
  
    if (!candidateIds) {
      // If no suitable index is found, perform a full collection scan to gather all document IDs
      candidateIds = await this.fullCollectionScan(collection);
    }
  
    // Convert the Set of candidate IDs to a sorted array for consistent ordering
    const sortedIds = Array.from(candidateIds).sort();
  
    let count = 0;
  
    // Iterate over each ID and evaluate the corresponding document against the query
    for (const id of sortedIds) {
      try {
        if (typeof id !== 'string') {
          // Skip invalid IDs
          continue;
        }
  
        // Read the document by its ID
        const doc = await this.read(collection, id);
  
        if (doc && Object.entries(structuredQuery).every(([field, condition]) =>
          evaluateCondition(doc, field, condition)
        )) {
          // Increment the count if the document satisfies all query conditions
          count += 1;
        }
      } catch (error) {
        if (error.statusCode === 404) {
          console.warn(`Document with id ${id} not found in collection ${collection}. It may have been deleted.`);
        } else {
          console.error(`Error reading document with id ${id} in collection ${collection}:`, error);
        }
        // Continue counting despite errors with individual documents
      }
    }
  
    console.log('count', count);
    return count;
  }
  

  async find(collection, query, options = {}) {
      const { limit = Infinity, offset = 0, after = null, analyze = false } = options;
      const structuredQuery = parseQuery(query);
      let storageReads = 0;
      let indexesUsed = new Set();
      let candidateIds;
      let numDocumentsRead = 0;
      
      // Check if the query is empty and no suitable index is found
    const isQueryEmpty = Object.keys(structuredQuery).length === 0;

    if (isQueryEmpty) {
      // Use efficient blob listing with pagination
      const containerClient = await this.getContainerClient(collection);
      let continuationToken = undefined;
      let processed = 0;
      const documents = [];

      // If 'after' is specified, set the continuationToken accordingly
      if (after) {
        continuationToken = after;
      }

      // Define the maximum number of documents to fetch
      const maxDocuments = limit === Infinity ? undefined : limit;

      // Use the byPage method to fetch blobs in pages
      const maxPageSize = 5000; // Adjust based on your performance needs
      const iter = containerClient.listBlobsFlat().byPage({ maxPageSize, continuationToken });

      for await (const response of iter) {
        storageReads += 1; // Each page is a storage read

        for (const blob of response.segment.blobItems) {
          // If 'after' is specified, skip blobs until after the specified ID
          if (after && blob.name <= after) {
            continue;
          }

          // Apply offset
          if (processed < offset) {
            processed += 1;
            continue;
          }

          // Read the document
          const doc = await this.read(collection, blob.name);
          numDocumentsRead += 1;

          if (doc) {
            documents.push(doc);
          }

          if (maxDocuments && documents.length >= maxDocuments) {
            break;
          }
        }

        if (maxDocuments && documents.length >= maxDocuments) {
          break;
        }

        // Update the continuationToken for the next iteration
        continuationToken = response.continuationToken;

        // If no more blobs, break the loop
        if (!continuationToken) {
          break;
        }
      }

      if (analyze) {
        return {
          indexesUsed: ['fullscan'],
          storageReads,
          estimatedDocumentsScanned: numDocumentsRead,
        };
      }

      return documents;
    }
    // Find the best index
    let indexBlobs = [];
    for await (const blob of this.indexContainer.listBlobsFlat({ prefix: `${collection}_` })) {
      indexBlobs.push(blob);
    }
    storageReads += 1; // Listing index blobs counts as one storage read
  
    let bestIndex = null;
    let maxFieldsMatched = 0;
  
    for (const indexBlob of indexBlobs) {
      const indexKey = indexBlob.name;
      const indexData = await this.getIndex(indexKey);
      storageReads += 1; // getIndex call counts as storage read
  
      if (!indexData) continue;
      const { type, fields } = indexData;
      const indexFields = Array.isArray(fields) ? fields : [fields];
  
      const fieldsMatched = indexFields.filter(field => field in structuredQuery).length;
  
      if (fieldsMatched > maxFieldsMatched) {
        maxFieldsMatched = fieldsMatched;
        bestIndex = { indexKey, indexFields, type };
      }
  
      if (fieldsMatched === Object.keys(structuredQuery).length) {
        break; // Found an index matching all query fields
      }
    }
  
    if (bestIndex) {
      const { indexKey, indexFields, type } = bestIndex;
  
      if (
        (type === 'compound' && indexFields.every(field => field in structuredQuery)) ||
        (type !== 'compound' && indexFields.length === 1 && indexFields.every(field => field in structuredQuery))
      ) {
        indexesUsed.add(indexKey);
  
        // Use the index directly
        const indexData = await this.getIndex(indexKey);
        storageReads += 1; // Reading index data
  
        const field = indexFields[0];
        const condition = structuredQuery[field];
        const ids = await this.lookupIndexIds(collection, field, condition);
        storageReads += 1; // lookupIndexIds reads index
  
        if (ids !== null) {
          candidateIds = new Set(ids);
          numDocumentsRead = candidateIds.size;
        } else {
          // Should not happen, since we have the index
          candidateIds = null;
          indexesUsed = new Set();
        }
      } else {
        // Combine single-field indexes using lookupIndexIds
        const candidateIdSets = [];
        for (const field of Object.keys(structuredQuery)) {
          const condition = structuredQuery[field];
          const ids = await this.lookupIndexIds(collection, field, condition);
          storageReads += 1; // lookupIndexIds reads index
  
          if (ids !== null) {
            indexesUsed.add(`${collection}_${field}`);
            candidateIdSets.push(new Set(ids));
          } else {
            // No index for this field
            candidateIds = null;
            indexesUsed = new Set();
            break;
          }
        }
  
        if (candidateIdSets.length > 0) {
          candidateIds = this.intersectSets(candidateIdSets);
          numDocumentsRead = candidateIds.size;
        }
      }
    }
  
    if (!candidateIds) {
      // No indexes or couldn't combine indexes, perform full scan
      candidateIds = await this.fullCollectionScan(collection);
      storageReads += 1; // fullCollectionScan reads blob list
  
      numDocumentsRead = candidateIds.size;
      indexesUsed = new Set(['fullscan']);
    }
  
    // Apply pagination
    let sortedIds = Array.from(candidateIds).sort();
    let startIndex = 0;
    if (after) {
      startIndex = sortedIds.findIndex(id => id > after) + 1;
    } else {
      startIndex = offset;
    }
    const paginatedIds = sortedIds.slice(startIndex, startIndex + limit);
  
    if (analyze) {
      // If analyze is true, return {
      return {
        indexesUsed: Array.from(indexesUsed),
        storageReads,
        estimatedDocumentsScanned: numDocumentsRead,
      };
    }
  
    // Fetch and filter the documents
    const documents = await Promise.all(
      paginatedIds.map(async id => {
        try {
          if (typeof id !== 'string') {
            // Skip invalid IDs
            return null;
          }
          storageReads += 1; // Reading each document counts as a storage read
          return await this.read(collection, id);
        } catch (error) {
          if (error.statusCode === 404) {
            console.warn(`Document with id ${id} not found in collection ${collection}. It may have been deleted.`);
          } else {
            console.error(`Error reading document with id ${id} in collection ${collection}:`, error);
          }
          return null;
        }
      })
    );
  
    // Filter out null documents and apply query filtering
    const filteredDocuments = documents
      .filter(doc => doc !== null)
      .filter(doc =>
        Object.entries(structuredQuery).every(([field, condition]) =>
          evaluateCondition(doc, field, condition)
        )
      );
  
    return filteredDocuments;
  }
  
  
  

  intersectSets(sets) {
    if (sets.length === 0) return new Set();
    return sets.reduce((a, b) => new Set([...a].filter(x => b.has(x))));
  }
  
  async lookupIndexIds(collection, field, condition) {
    const indexKey = `${collection}_${field}`;
    const indexData = await this.getIndex(indexKey);
    if (!indexData) return null;
  
    const { type } = indexData;
    if (type === 'date') {
      const matchingIds = await this.lookupDateIndex(collection, indexData, condition);
      return matchingIds;
    } else {
      // Default index
      const index = indexData.index;
      let matchingIds = [];
      if (condition.operator === 'EQ') {
        const value = condition.value;
        matchingIds = index[value] || [];
      } else {
        for (const [value, ids] of Object.entries(index)) {
          if (evaluateCondition({ [field]: value }, field, condition)) {
            matchingIds.push(...ids);
          }
        }
      }
      return matchingIds;
    }
  }
  

  async lookupCompoundIndex(collection, indexKey, structuredQuery) {
    const indexData = await this.getIndex(collection, indexKey);
    if (!indexData) return [];
  
    const { index, fields } = indexData;
    const indexValue = this.getCompoundIndexQueryValue(structuredQuery, fields);
    const matchingIds = index[indexValue] || [];
    return new Set(matchingIds);
  }
  
  getCompoundIndexQueryValue(query, fields) {
    const values = fields.map(field => {
      const condition = query[field];
      if (!condition || condition.operator !== 'EQ') {
        throw new Error(`Compound index requires equality condition on field: ${field}`);
      }
      return condition.value;
    });
    return values.join('_');
  }

  async createIndex(collection, fields, options = {}) {
    const {
      unique = false,
      createOnlyIfNotExists = false,
      type = 'default',
      granularity = null,
    } = options;
  
    const indexName = Array.isArray(fields) ? fields.join('_') : fields;
    const indexKey = `${collection}_${indexName}`;
    const indexBlobClient = this.indexContainer.getBlockBlobClient(indexKey);
  
    // Prepare index data
    let indexData = {
      type,
      fields,
      unique,
      granularity,
      index: {},
    };
  
    // Create index based on type
    if (type === 'date') {
      await this.createDateIndex(collection, fields, indexBlobClient, indexData);
    } else if (type === 'compound') {
      await this.createCompoundIndex(collection, fields, indexBlobClient, indexData);
    } else {
      await this.createDefaultIndex(collection, fields, indexBlobClient, indexData);
    }
  
    // Attempt to create the index blob atomically
    try {
      await indexBlobClient.upload(
        JSON.stringify(indexData),
        Buffer.byteLength(JSON.stringify(indexData)),
        {
          conditions: { ifNoneMatch: '*' },
        }
      );
      console.log(`Index for ${indexKey} in ${collection} has been created.`);
    } catch (error) {
      if (error.code === 'BlobAlreadyExists' || error.statusCode === 409) {
        // Index blob already exists
        if (createOnlyIfNotExists) {
          console.log(`Index for ${indexKey} in ${collection} already exists. Skipping creation.`);
          return;
        } else {
          console.log(`Index for ${indexKey} in ${collection} already exists. Overwriting.`);
          await indexBlobClient.upload(
            JSON.stringify(indexData),
            Buffer.byteLength(JSON.stringify(indexData))
            // No conditions here to allow overwriting
          );
        }
      } else {
        throw error;
      }
    }
  
    // Update unique constraints
    if (unique) {
      if (!this.uniqueConstraints[collection]) {
        this.uniqueConstraints[collection] = [];
      }
      this.uniqueConstraints[collection].push(fields);
    }
  }
  
  async createDefaultIndex(collection, fields, indexBlobClient, indexData) {
    const indexKey = Array.isArray(fields) ? fields.join('_') : fields;
    const index = {};
    const containerClient = await this.getContainerClient(collection);
    const blobs = containerClient.listBlobsFlat();
  
    for await (const blob of blobs) {
      const doc = await this.read(collection, blob.name);
      const value = this.getIndexValue(doc, fields);
      if (value !== undefined) {
        if (indexData.unique && index[value]) {
          throw new Error(`Unique constraint violation for fields ${indexKey} with value ${value}`);
        }
        if (!index[value]) {
          index[value] = [];
        }
        index[value].push(blob.name);
      }
    }
  
    indexData.index = index;
  
    // Attempt to create the index blob atomically
    try {
      await indexBlobClient.upload(
        JSON.stringify(indexData),
        Buffer.byteLength(JSON.stringify(indexData)),
        {
          conditions: { ifNoneMatch: '*' },
        }
      );
      console.log(`Index for ${indexKey} in ${collection} has been created.`);
    } catch (error) {
      if (error.code === 'BlobAlreadyExists' || error.statusCode === 409) {
        // Index blob already exists
        if (indexData.createOnlyIfNotExists) {
          console.log(`Index for ${indexKey} in ${collection} already exists. Skipping creation.`);
          return;
        } else {
          console.log(`Index for ${indexKey} in ${collection} already exists. Overwriting.`);
          await indexBlobClient.upload(
            JSON.stringify(indexData),
            Buffer.byteLength(JSON.stringify(indexData))
          );
        }
      } else {
        throw error;
      }
    }
  
    // Update unique constraints
    if (indexData.unique) {
      if (!this.uniqueConstraints[collection]) {
        this.uniqueConstraints[collection] = [];
      }
      this.uniqueConstraints[collection].push(fields);
    }
  }


  async createCompoundIndex(collection, fields, indexBlobClient, indexData) {
    const index = {};
    const containerClient = await this.getContainerClient(collection);
    const blobs = containerClient.listBlobsFlat();
  
    for await (const blob of blobs) {
      const doc = await this.read(collection, blob.name);
      const value = this.getCompoundIndexValue(doc, fields);
      if (value !== undefined) {
        if (!index[value]) {
          index[value] = [];
        }
        index[value].push(blob.name);
      }
    }
  
    indexData.index = index;
  
    // Attempt to create the index blob atomically
    try {
      await indexBlobClient.upload(
        JSON.stringify(indexData),
        Buffer.byteLength(JSON.stringify(indexData)),
        {
          conditions: { ifNoneMatch: '*' },
        }
      );
      console.log(`Compound index for ${fields.join(', ')} in ${collection} has been created.`);
    } catch (error) {
      if (error.code === 'BlobAlreadyExists' || error.statusCode === 409) {
        // Index blob already exists
        if (indexData.createOnlyIfNotExists) {
          console.log(`Index for ${fields.join(', ')} in ${collection} already exists. Skipping creation.`);
          return;
        } else {
          console.log(`Index for ${fields.join(', ')} in ${collection} already exists. Overwriting.`);
          await indexBlobClient.upload(
            JSON.stringify(indexData),
            Buffer.byteLength(JSON.stringify(indexData))
          );
        }
      } else {
        throw error;
      }
    }
  }

  getCompoundIndexValue(doc, fields) {
    const values = fields.map(field => {
      const value = doc[field];
      if (value === undefined) return null;
      return value;
    });
    if (values.includes(null)) return undefined;
    return values.join('_');
  }

  

  async createDateIndex(collection, fields, indexBlobClient, indexData) {
    const indexEntries = [];
    const containerClient = await this.getContainerClient(collection);
    const blobs = containerClient.listBlobsFlat();
  
    for await (const blob of blobs) {
      const doc = await this.read(collection, blob.name);
      const value = this.getIndexValue(doc, fields);
      if (value !== undefined) {
        const dateValue = this.formatDateValue(value, indexData.granularity);
        let entry = indexEntries.find(e => e.dateValue === dateValue);
        if (!entry) {
          entry = { dateValue, ids: [] };
          indexEntries.push(entry);
        }
        entry.ids.push(blob.name);
      }
    }
  
    // Sort indexEntries by dateValue
    indexEntries.sort((a, b) => a.dateValue.localeCompare(b.dateValue));
  
    indexData.index = indexEntries;
  
    // Attempt to create the index blob atomically
    try {
      await indexBlobClient.upload(
        JSON.stringify(indexData),
        Buffer.byteLength(JSON.stringify(indexData)),
        {
          conditions: { ifNoneMatch: '*' },
        }
      );
      console.log(`Date index for ${fields} in ${collection} has been created.`);
    } catch (error) {
      if (error.code === 'BlobAlreadyExists' || error.statusCode === 409) {
        // Index blob already exists
        if (indexData.createOnlyIfNotExists) {
          console.log(`Date index for ${fields} in ${collection} already exists. Skipping creation.`);
          return;
        } else {
          console.log(`Date index for ${fields} in ${collection} already exists. Overwriting.`);
          await indexBlobClient.upload(
            JSON.stringify(indexData),
            Buffer.byteLength(JSON.stringify(indexData))
          );
        }
      } else {
        throw error;
      }
    }
  }

  formatDateValue(value, granularity) {
    const date = new Date(value);
    if (isNaN(date.getTime())) {
      throw new Error(`Invalid date value: ${value}`);
    }
    switch (granularity) {
      case 'daily':
        return date.toISOString().slice(0, 10); // 'YYYY-MM-DD'
      case 'weekly':
        const weekNumber = this.getWeekNumber(date);
        return `${date.getUTCFullYear()}-W${weekNumber}`;
      case 'monthly':
        return date.toISOString().slice(0, 7); // 'YYYY-MM'
      default:
        return date.toISOString();
    }
  }

  getWeekNumber(date) {
    const oneJan = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
    const numberOfDays = Math.floor((date - oneJan) / (24 * 60 * 60 * 1000));
    return Math.ceil((numberOfDays + oneJan.getUTCDay() + 1) / 7);
  }

  getIndexValue(doc, fields) {
    if (Array.isArray(fields)) {
      const values = fields.map(field => doc[field]);
      if (values.includes(undefined)) return undefined;
      return values.join('_');
    }
    return doc[fields];
  }

  async getIndex(indexKey) {
    console.log('Attempting to get index:', indexKey);
    const indexBlobClient = this.indexContainer.getBlockBlobClient(indexKey);
    try {
      const downloadResponse = await indexBlobClient.download();
      const downloaded = await streamToBuffer(downloadResponse.readableStreamBody);
      console.log('Index found:', indexKey);
      return JSON.parse(downloaded.toString());
    } catch (error) {
      console.log('Error getting index:', error.message);
      if (error.statusCode === 404) {
        return null;
      }
      throw error;
    }
  }

  async updateIndexes(collection, id, newData, oldData) {
    console.log('Updating indexes for collection:', collection);

    const indexBlobs = this.indexContainer.listBlobsFlat({ prefix: `${collection}_` });

    for await (const indexBlob of indexBlobs) {
      const indexKey = indexBlob.name;
      console.log('Processing index:', indexKey);
      let indexData = await this.getIndexWithETag(indexKey);
      if (!indexData) {
        console.warn(`Index ${indexKey} not found. Skipping.`);
        continue;
      }
  
      let { unique, type, fields, eTag } = indexData;
  
      // Implement concurrency control
      const maxRetries = 5;
      let retries = 0;
      let success = false;
  
      while (!success && retries < maxRetries) {
        try {
          if (type === 'date') {
            await this.updateDateIndex(indexData, id, newData, oldData, fields, indexBlob, eTag);
          } else if (type === 'compound') {
            await this.updateCompoundIndex(indexData, id, newData, oldData, fields, indexBlob, eTag);
          } else {
            await this.updateDefaultIndex(indexData, id, newData, oldData, fields, indexBlob, unique, eTag);
          }
          success = true;
        } catch (error) {
          if (error.code === 'BlobAlreadyExists' || error.statusCode === 409 || error.statusCode === 412) {
            // ETag mismatch, read the latest index data and retry
            retries++;
            const delay = Math.pow(2, retries) * 100; // Delay in milliseconds
            await new Promise(resolve => setTimeout(resolve, delay));

            indexData = await this.getIndexWithETag(collection, indexKey);
            if (!indexData) {
              console.warn(`Index ${indexKey} was deleted during update. Skipping.`);
              break;
            }
            eTag = indexData.eTag;
          } else {
            throw error;
          }
        }
      }
  
      if (!success && retries === maxRetries) {
        console.error(`Failed to update index ${indexKey} after ${maxRetries} retries due to concurrent modifications.`);
      }
    }
  }

  async getIndexWithETag(indexKey) {
    console.log(`Getting index with ETag: ${indexKey}`);
    const indexBlobClient = this.indexContainer.getBlockBlobClient(indexKey);
    try {
      const downloadResponse = await indexBlobClient.download();
      const downloaded = await streamToBuffer(downloadResponse.readableStreamBody);
      const indexData = JSON.parse(downloaded.toString());
      indexData.eTag = downloadResponse.etag;
      console.log(`Index found: ${indexKey}`);
      return indexData;
    } catch (error) {
      console.log(`Error getting index: ${indexKey}`, error.message);
      if (error.statusCode === 404) {
        return null;
      }
      throw error;
    }
  }

  async updateDefaultIndex(indexData, id, newData, oldData, fields, indexBlob, unique, eTag) {
    const { index } = indexData;
  
    // Remove old data
    if (oldData) {
      const oldValue = this.getIndexValue(oldData, fields);
      if (oldValue !== undefined && index[oldValue]) {
        index[oldValue] = index[oldValue].filter(docId => docId !== id);
        if (index[oldValue].length === 0) {
          delete index[oldValue];
        }
      }
    }
  
    // Add new data
    const newValue = this.getIndexValue(newData, fields);
    if (newValue !== undefined) {
      if (unique && index[newValue] && index[newValue].length > 0) {
        throw new Error(`Unique constraint violation for fields ${fields} with value ${newValue}`);
      }
      if (!index[newValue]) {
        index[newValue] = [];
      }
      if (!index[newValue].includes(id)) {
        index[newValue].push(id);
      }
    }
  
    // Save updated index with ETag condition
    await this.indexContainer.getBlockBlobClient(indexBlob.name).upload(
      JSON.stringify({ type: 'default', fields, unique, index }),
      Buffer.byteLength(JSON.stringify({ type: 'default', fields, unique, index })),
      {
        conditions: { ifMatch: eTag },
      }
    );
  }

  async updateCompoundIndex(indexData, id, newData, oldData, fields, indexBlob, eTag) {
  const { index } = indexData;

  // Remove old data
  if (oldData) {
    const oldValue = this.getCompoundIndexValue(oldData, fields);
    if (oldValue !== undefined && index[oldValue]) {
      index[oldValue] = index[oldValue].filter(docId => docId !== id);
      if (index[oldValue].length === 0) {
        delete index[oldValue];
      }
    }
  }

  // Add new data
  const newValue = this.getCompoundIndexValue(newData, fields);
  if (newValue !== undefined) {
    if (!index[newValue]) {
      index[newValue] = [];
    }
    if (!index[newValue].includes(id)) {
      index[newValue].push(id);
    }
  }

  // Save updated index with ETag condition
  await this.indexContainer.getBlockBlobClient(indexBlob.name).upload(
    JSON.stringify({ type: 'compound', fields, index }),
    Buffer.byteLength(JSON.stringify({ type: 'compound', fields, index })),
    {
      conditions: { ifMatch: eTag },
    }
  );
}

async updateDateIndex(indexData, id, newData, oldData, fields, indexBlob, eTag) {
  const { index, granularity } = indexData;

  // Remove old data
  if (oldData) {
    const oldValue = this.getIndexValue(oldData, fields);
    if (oldValue !== undefined) {
      const oldDateValue = this.formatDateValue(oldValue, granularity);
      const entryIndex = index.findIndex(e => e.dateValue === oldDateValue);
      if (entryIndex !== -1) {
        const ids = index[entryIndex].ids;
        const idIndex = ids.indexOf(id);
        if (idIndex !== -1) {
          ids.splice(idIndex, 1);
          if (ids.length === 0) {
            index.splice(entryIndex, 1);
          }
        }
      }
    }
  }

  // Add new data
  const newValue = this.getIndexValue(newData, fields);
  if (newValue !== undefined) {
    const newDateValue = this.formatDateValue(newValue, granularity);
    let entry = index.find(e => e.dateValue === newDateValue);
    if (!entry) {
      entry = { dateValue: newDateValue, ids: [] };
      index.push(entry);
      // Keep index sorted
      index.sort((a, b) => a.dateValue.localeCompare(b.dateValue));
    }
    if (!entry.ids.includes(id)) {
      entry.ids.push(id);
    }
  }

  // Save updated index with ETag condition
  await this.indexContainer.getBlockBlobClient(indexBlob.name).upload(
    JSON.stringify({ type: 'date', fields, granularity, index }),
    Buffer.byteLength(JSON.stringify({ type: 'date', fields, granularity, index })),
    {
      conditions: { ifMatch: eTag },
    }
  );
}

  async removeFromIndexes(collection, id, oldData) {
    const indexBlobs = this.indexContainer.listBlobsFlat({ prefix: `${collection}_` });
    for await (const indexBlob of indexBlobs) {
      const indexKey = indexBlob.name;
      let indexData = await this.getIndexWithETag(indexKey);
      if (!indexData) {
        console.warn(`Index ${indexKey} not found. Skipping.`);
        continue;
      }
      const { type, fields, eTag } = indexData;
  
      const maxRetries = 5;
      let retries = 0;
      let success = false;
  
      while (!success && retries < maxRetries) {
        try {
          if (type === 'date') {
            await this.removeFromDateIndex(indexData, id, oldData, fields, indexBlob, eTag);
          } else if (type === 'compound') {
            await this.removeFromCompoundIndex(indexData, id, oldData, fields, indexBlob, eTag);
          } else {
            await this.removeFromDefaultIndex(indexData, id, oldData, fields, indexBlob, eTag);
          }
          success = true;
        } catch (error) {
          if (error.statusCode === 412) {
            retries++;
            const delay = Math.pow(2, retries) * 100;
            await new Promise(resolve => setTimeout(resolve, delay));
            indexData = await this.getIndexWithETag(collection, indexKey);
            eTag = indexData.eTag;
          } else {
            throw error;
          }
        }
      }
  
      if (!success) {
        throw new Error(`Failed to remove from index ${indexKey} after ${maxRetries} retries due to concurrent modifications.`);
      }
    }
  }
  
  async removeFromDefaultIndex(indexData, id, oldData, fields, indexBlob, eTag) {
    const { index, unique } = indexData;
  
    // Get the old index value
    const oldValue = this.getIndexValue(oldData, fields);
    if (oldValue !== undefined && index[oldValue]) {
      index[oldValue] = index[oldValue].filter(docId => docId !== id);
      if (index[oldValue].length === 0) {
        delete index[oldValue];
      }
  
      // Save updated index with ETag condition
      await this.indexContainer.getBlockBlobClient(indexBlob.name).upload(
        JSON.stringify({ type: 'default', fields, unique, index }),
        Buffer.byteLength(JSON.stringify({ type: 'default', fields, unique, index })),
        {
          conditions: { ifMatch: eTag },
        }
      );
    }
  }

  async removeFromCompoundIndex(indexData, id, oldData, fields, indexBlob) {
    const { index } = indexData;
  
    const oldValue = this.getCompoundIndexValue(oldData, fields);
    if (oldValue !== undefined) {
      index[oldValue] = index[oldValue].filter(docId => docId !== id);
      if (index[oldValue].length === 0) {
        delete index[oldValue];
      }
  
      // Save updated index
      await this.indexContainer.getBlockBlobClient(indexBlob.name).upload(
        JSON.stringify(indexData),
        JSON.stringify(indexData).length
      );
    }
  }

  async removeFromDateIndex(indexData, id, oldData, fields, indexBlob) {
    const { index, granularity } = indexData;
    const oldValue = this.getIndexValue(oldData, fields);
    if (oldValue !== undefined) {
      const oldDateValue = this.formatDateValue(oldValue, granularity);
      const entryIndex = index.findIndex(e => e.dateValue === oldDateValue);
      if (entryIndex !== -1) {
        const ids = index[entryIndex].ids;
        const idIndex = ids.indexOf(id);
        if (idIndex !== -1) {
          ids.splice(idIndex, 1);
          if (ids.length === 0) {
            index.splice(entryIndex, 1);
          }
        }
      }
      // Save updated index
      await this.indexContainer.getBlockBlobClient(indexBlob.name).upload(
        JSON.stringify(indexData),
        JSON.stringify(indexData).length
      );
    }
  }

  

  async findBestIndex(collection, structuredQuery) {
    const indexBlobs = this.indexContainer.listBlobsFlat({ prefix: `${collection}_` });
    let bestIndex = null;
    let maxFieldsMatched = 0;
  
    for await (const indexBlob of indexBlobs) {
      const indexKey = indexBlob.name;
      const indexData = await this.getIndex(indexKey);
      if (!indexData) continue; // Skip if index data is not found
      const { type, fields } = indexData;
      const indexFields = Array.isArray(fields) ? fields : [fields];
  
      const fieldsMatched = indexFields.filter(field => field in structuredQuery).length;
  
      if (fieldsMatched > maxFieldsMatched) {
        maxFieldsMatched = fieldsMatched;
        bestIndex = { indexKey, indexFields, type };
      }
  
      if (fieldsMatched === Object.keys(structuredQuery).length) {
        break; // Found an index matching all query fields
      }
    }
  
    return bestIndex;
  }

  async lookupIndex(collection, field, condition) {
    const indexData = await this.getIndex(collection, field);
    if (!indexData) return [];

    const { type } = indexData;
    if (type === 'date') {
      return this.lookupDateIndex(collection, indexData, condition);
    } else {
      // Existing logic for non-date indexes
      const index = indexData.index;
      let matchingIds = [];
      if (condition.operator === 'EQ') {
        const value = condition.value;
        matchingIds = index[value] || [];
      } else {
        for (const [value, ids] of Object.entries(index)) {
          if (evaluateCondition({ [field]: value }, field, condition)) {
            matchingIds.push(...ids);
          }
        }
      }
      return Promise.all(matchingIds.map(id => this.read(collection, id)));
    }
  }

  async lookupDateIndex(collection, indexData, condition) {
    const { index, granularity } = indexData;
    const entries = index; // entries is an array of { dateValue, ids }
    let matchingIds = [];
  
    const formatConditionValue = (value) => this.formatDateValue(value, granularity);
  
    // Implement binary search functions
    function binarySearchLeft(arr, target) {
      let left = 0;
      let right = arr.length;
      while (left < right) {
        let mid = Math.floor((left + right) / 2);
        if (arr[mid].dateValue < target) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      return left;
    }
  
    function binarySearchRight(arr, target) {
      let left = 0;
      let right = arr.length;
      while (left < right) {
        let mid = Math.floor((left + right) / 2);
        if (arr[mid].dateValue <= target) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      return left;
    }
  
    if (condition.operator === 'EQ') {
      const formattedValue = formatConditionValue(condition.value);
      const idx = binarySearchLeft(entries, formattedValue);
      if (idx < entries.length && entries[idx].dateValue === formattedValue) {
        matchingIds = entries[idx].ids;
      }
    } else if (condition.operator === 'GT') {
      const formattedValue = formatConditionValue(condition.value);
      const idx = binarySearchRight(entries, formattedValue);
      for (let i = idx; i < entries.length; i++) {
        matchingIds.push(...entries[i].ids);
      }
    } else if (condition.operator === 'GTE') {
      const formattedValue = formatConditionValue(condition.value);
      const idx = binarySearchLeft(entries, formattedValue);
      for (let i = idx; i < entries.length; i++) {
        matchingIds.push(...entries[i].ids);
      }
    } else if (condition.operator === 'LT') {
      const formattedValue = formatConditionValue(condition.value);
      const idx = binarySearchLeft(entries, formattedValue);
      for (let i = 0; i < idx; i++) {
        matchingIds.push(...entries[i].ids);
      }
    } else if (condition.operator === 'LTE') {
      const formattedValue = formatConditionValue(condition.value);
      const idx = binarySearchRight(entries, formattedValue);
      for (let i = 0; i < idx; i++) {
        matchingIds.push(...entries[i].ids);
      }
    } else if (condition.operator === 'BETWEEN') {
      const [startValue, endValue] = condition.value;
      const formattedStartValue = formatConditionValue(startValue);
      const formattedEndValue = formatConditionValue(endValue);
      const startIdx = binarySearchLeft(entries, formattedStartValue);
      const endIdx = binarySearchRight(entries, formattedEndValue);
      for (let i = startIdx; i < endIdx; i++) {
        matchingIds.push(...entries[i].ids);
      }
    }
  
    return matchingIds;
  }

  async fullCollectionScan(collection) {
    const containerClient = await this.getContainerClient(collection);
    const blobs = containerClient.listBlobsFlat();
    const ids = new Set();
    for await (const blob of blobs) {
      ids.add(blob.name);
    }
    return ids;
  }

  async listCollections() {
    const containers = this.blobServiceClient.listContainers();
    const containerNames = [];
    for await (const container of containers) {
      if (container.name !== 'indexes'){
        containerNames.push(container.name);
      }
    }
    return containerNames;
  }

  async dropCollection(collection) {
    const containerClient = this.blobServiceClient.getContainerClient(collection);
    await containerClient.delete();
  }
}

async function streamToBuffer(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on('data', (data) => {
      chunks.push(data instanceof Buffer ? data : Buffer.from(data));
    });
    readableStream.on('end', () => {
      resolve(Buffer.concat(chunks));
    });
    readableStream.on('error', reject);
  });
}

module.exports = AzureBlobStorage;
