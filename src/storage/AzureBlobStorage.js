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

  async create(collection, id, data) {
    if (!id) {
      id = ulid();
    }
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
      const indexKey = Array.isArray(fields) ? fields.join('_') : fields;
      const fullIndexKey = `${collection}_${indexKey}`;
      const indexData = await this.getIndex(collection, indexKey);
      if (indexData && indexData.unique) {
        const value = this.getIndexValue(data, fields);
        if (value !== undefined && indexData.index[value] && indexData.index[value].length > 0) {
          throw new Error(`Unique constraint violation for fields ${indexKey} with value ${value}`);
        }
      }
    }
  }

  async read(collection, id) {
    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);
    const downloadResponse = await blobClient.download();
    const downloaded = await streamToBuffer(downloadResponse.readableStreamBody);
    return JSON.parse(downloaded.toString());
  }

  async update(collection, id, data) {
    const oldData = await this.read(collection, id);
    await this.checkUniqueConstraints(collection, data);
    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);
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

  async find(collection, query, options = {}) {
    const { limit = Infinity, offset = 0 } = options;
    const structuredQuery = parseQuery(query);
    const bestIndex = await this.findBestIndex(collection, structuredQuery);
  
    let candidateIds;
  
    if (bestIndex) {
      const { indexKey, indexFields, type } = bestIndex;
      if (type === 'compound' && indexFields.every(field => field in structuredQuery)) {
        // Use compound index
        candidateIds = await this.lookupCompoundIndex(collection, indexKey, structuredQuery);
      } else {
        // Combine single-field indexes
        const candidateIdSets = [];
        for (const field of Object.keys(structuredQuery)) {
          const ids = await this.lookupIndexIds(collection, field, structuredQuery[field]);
          if (ids) {
            candidateIdSets.push(new Set(ids));
          } else {
            // If any field doesn't have an index, we need to do a full scan
            candidateIds = null;
            break;
          }
        }
  
        if (candidateIdSets.length > 0) {
          candidateIds = this.intersectSets(candidateIdSets);
        }
      }
    }
  
    let candidateDocuments;
    if (candidateIds) {
      candidateDocuments = await Promise.all([...candidateIds].map(id => this.read(collection, id)));
    } else {
      // No indexes or couldn't combine indexes, perform full scan
      candidateDocuments = await this.fullCollectionScan(collection);
    }
  
    const filteredDocuments = candidateDocuments.filter(doc =>
      Object.entries(structuredQuery).every(([field, condition]) =>
        evaluateCondition(doc, field, condition)
      )
    );
  
    return filteredDocuments.slice(offset, offset + limit);
  }

  intersectSets(sets) {
    if (sets.length === 0) return new Set();
    return sets.reduce((a, b) => new Set([...a].filter(x => b.has(x))));
  }
  
  async lookupIndexIds(collection, field, condition) {
    const indexData = await this.getIndex(collection, field);
    if (!indexData) return null;
  
    const { type } = indexData;
    if (type === 'date') {
      const docs = await this.lookupDateIndex(indexData, condition);
      return docs.map(doc => doc.id);
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
      return fields.map(field => doc[field]).join('_');
    }
    return doc[fields];
  }

  async getIndex(collection, field) {
    const indexBlobClient = this.indexContainer.getBlockBlobClient(`${collection}_${field}`);
    try {
      const downloadResponse = await indexBlobClient.download();
      const downloaded = await streamToBuffer(downloadResponse.readableStreamBody);
      return JSON.parse(downloaded.toString());
    } catch (error) {
      if (error.statusCode === 404) {
        return null;
      }
      throw error;
    }
  }

  async updateIndexes(collection, id, newData, oldData) {
    const indexBlobs = this.indexContainer.listBlobsFlat({ prefix: `${collection}_` });
    for await (const indexBlob of indexBlobs) {
      const indexKey = indexBlob.name; // Use the full blob name as the indexKey
      let indexData = await this.getIndexWithETag(collection, indexKey);
      
      // Skip this index if it doesn't exist
      if (!indexData) {
        console.warn(`Index ${indexKey} not found for collection ${collection}. Skipping.`);
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
    const indexBlobClient = this.indexContainer.getBlockBlobClient(indexKey);
    try {
      const downloadResponse = await indexBlobClient.download();
      const downloaded = await streamToBuffer(downloadResponse.readableStreamBody);
      const indexData = JSON.parse(downloaded.toString());
      indexData.eTag = downloadResponse.etag; // Get ETag
      return indexData;
    } catch (error) {
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
      const indexKey = indexBlob.name.substring(this.indexContainer.containerName.length + 1);
      let indexData = await this.getIndexWithETag(collection, indexKey);
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
  let bestIndexFields = [];
  let maxFieldsMatched = 0;

  for await (const indexBlob of indexBlobs) {
    const indexKey = indexBlob.name.substring(this.indexContainer.containerName.length + 1);
    const indexData = await this.getIndex(collection, indexKey);
    const { type, fields } = indexData;

    let indexFields;
    if (type === 'compound' || Array.isArray(fields)) {
      indexFields = fields;
    } else {
      indexFields = [indexKey.split('_').slice(1).join('_')];
    }

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
      return this.lookupDateIndex(indexData, condition);
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

  async lookupDateIndex(indexData, condition) {
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

    return Promise.all(matchingIds.map(id => this.read(collection, id)));
  }

  async fullCollectionScan(collection) {
    const containerClient = await this.getContainerClient(collection);
    const blobs = containerClient.listBlobsFlat();
    const documents = [];
    for await (const blob of blobs) {
      const doc = await this.read(collection, blob.name);
      documents.push(doc);
    }
    return documents;
  }

  async listCollections() {
    const containers = this.blobServiceClient.listContainers();
    const containerNames = [];
    for await (const container of containers) {
      containerNames.push(container.name);
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
