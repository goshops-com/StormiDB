// src/storage/AzureBlobStorage.js

const { BlobServiceClient } = require('@azure/storage-blob');
const { parseQuery, operatorToTagCondition } = require('../query/QueryParser');
const { ulid } = require('ulid');
const { encodeTagValue, decodeTagValue, hashTagValue } = require('./tagEncoding');
const crypto = require('crypto');

class AzureBlobStorage {
  constructor(connectionString, options = {}) {
    this.prefix = options.prefix || '';
    this.blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    this.indexDefinitions = {}; // Cache for index definitions per collection
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

  async getContainerClient(collection) {
    const containerName = this.sanitizeContainerName(collection);
    const containerClient = this.blobServiceClient.getContainerClient(containerName);
    await containerClient.createIfNotExists();
    return containerClient;
  }

  async createIndex(collection, fields, options = {}) {
    if (!Array.isArray(fields)) {
      fields = [fields];
    }

    const { unique = false } = options;

    // Load existing index definitions
    const indexDefs = await this.loadIndexDefinitions(collection);

    // Update index definitions
    for (const field of fields) {
      // Check if field is already indexed
      if (indexDefs.indexedFields.has(field)) {
        // Update unique constraint if needed
        if (unique) {
          indexDefs.uniqueFields.add(field);
        }
      } else {
        if (indexDefs.indexedFields.size >= 10) {
          throw new Error(`Cannot index more than 10 fields per collection due to tag limit.`);
        }
        indexDefs.indexedFields.add(field);
        if (unique) {
          indexDefs.uniqueFields.add(field);
        }
      }
    }

    // Save updated index definitions
    await this.saveIndexDefinitions(collection, indexDefs);
  }

  async loadIndexDefinitions(collection) {
    if (this.indexDefinitions[collection]) {
      return this.indexDefinitions[collection];
    }

    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient('__collection_indexes');

    try {
      const downloadResponse = await blobClient.download();
      const downloaded = await streamToBuffer(downloadResponse.readableStreamBody);
      const indexDefs = JSON.parse(downloaded.toString());

      // Convert arrays back to sets
      indexDefs.indexedFields = new Set(indexDefs.indexedFields);
      indexDefs.uniqueFields = new Set(indexDefs.uniqueFields);
      indexDefs.eTag = downloadResponse.etag;

      // Cache the index definitions
      this.indexDefinitions[collection] = indexDefs;
      return indexDefs;
    } catch (error) {
      if (error.statusCode === 404) {
        // No index definitions exist yet
        const indexDefs = {
          indexedFields: new Set(),
          uniqueFields: new Set(),
          eTag: undefined,
        };
        this.indexDefinitions[collection] = indexDefs;
        return indexDefs;
      } else {
        throw error;
      }
    }
  }

  async saveIndexDefinitions(collection, indexDefs) {
    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient('__collection_indexes');

    // Prepare data for storage
    const data = {
      indexedFields: Array.from(indexDefs.indexedFields),
      uniqueFields: Array.from(indexDefs.uniqueFields),
    };

    const content = JSON.stringify(data);
    const contentLength = Buffer.byteLength(content);

    // Implement concurrency control with ETag
    const options = {};
    if (indexDefs.eTag) {
      options.conditions = { ifMatch: indexDefs.eTag };
    } else {
      options.conditions = { ifNoneMatch: '*' };
    }

    try {
      const uploadResponse = await blobClient.upload(content, contentLength, options);
      // Update eTag
      indexDefs.eTag = uploadResponse.etag;
      // Update cache
      this.indexDefinitions[collection] = indexDefs;
    } catch (error) {
      if (error.statusCode === 412 || error.statusCode === 409) {
        // ETag mismatch; reload index definitions and throw error to prompt retry
        await this.loadIndexDefinitions(collection);
        throw new Error(
          `Concurrent modification detected while updating index definitions for collection "${collection}". Please retry the operation.`
        );
      } else {
        throw error;
      }
    }
  }

  async create(collection, data, existingId = undefined) {
    const id = existingId || ulid();
    data.id = id;

    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);

    // Load index definitions
    const indexDefs = await this.loadIndexDefinitions(collection);

    // Prepare tags with only indexed fields
    const tags = this.prepareTags(collection, data, indexDefs);

    // Check for unique constraints
    await this.checkUniqueConstraintsOnCreate(collection, data, tags, indexDefs);

    await blobClient.upload(JSON.stringify(data), Buffer.byteLength(JSON.stringify(data)), {
      tags,
    });

    return id;
  }

  async checkUniqueConstraintsOnCreate(collection, data, tags, indexDefs) {
    if (!indexDefs.uniqueFields || indexDefs.uniqueFields.size === 0) {
      return;
    }

    const containerClient = await this.getContainerClient(collection);

    for (const field of indexDefs.uniqueFields) {
      const value = data[field];

      if (value === undefined || value === null) {
        continue; // Skip undefined or null values
      }

      const encodedValue = this.encodeTagValueForField(field, value);

      const tagFilter = `"${field}" = '${encodedValue.replace(/'/g, "''")}'`;

      const iterator = containerClient.findBlobsByTags(tagFilter);

      for await (const blob of iterator) {
        throw new Error(`Unique constraint violation: A document with the same "${field}" already exists.`);
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
      console.error('Error reading blob:', error);
      return null;
    }
  }

  async update(collection, id, data) {
    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);

    // Check if the blob exists
    const exists = await blobClient.exists();
    if (!exists) {
      throw new Error(`Document with id ${id} does not exist in collection ${collection}.`);
    }

    // Read the existing data
    const existingData = await this.read(collection, id);

    // Load index definitions
    const indexDefs = await this.loadIndexDefinitions(collection);

    data.id = id;

    // Prepare tags with only indexed fields
    const tags = this.prepareTags(collection, data, indexDefs);

    // Check for unique constraints
    await this.checkUniqueConstraintsOnUpdate(collection, data, existingData, indexDefs);

    // Overwrite the blob with new data and tags
    await blobClient.upload(JSON.stringify(data), Buffer.byteLength(JSON.stringify(data)), {
      overwrite: true,
      tags,
    });
  }

  async checkUniqueConstraintsOnUpdate(collection, newData, existingData, indexDefs) {
    if (!indexDefs.uniqueFields || indexDefs.uniqueFields.size === 0) {
      return;
    }

    const containerClient = await this.getContainerClient(collection);

    for (const field of indexDefs.uniqueFields) {
      const newValue = newData[field];
      const oldValue = existingData[field];

      if (newValue === oldValue) {
        continue; // Value hasn't changed; no need to check
      }

      if (newValue === undefined || newValue === null) {
        continue; // Skip undefined or null values
      }

      const encodedValue = this.encodeTagValueForField(field, newValue);

      const tagFilter = `"${field}" = '${encodedValue.replace(/'/g, "''")}'`;

      const iterator = containerClient.findBlobsByTags(tagFilter);

      for await (const blob of iterator) {
        if (blob.name !== newData.id) {
          throw new Error(`Unique constraint violation: A document with the same "${field}" already exists.`);
        }
      }
    }
  }

  async delete(collection, id) {
    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);

    // Delete the blob
    await blobClient.deleteIfExists();
  }

  prepareTags(collection, data, indexDefs) {
    const tags = {};
    const indexedFields = indexDefs.indexedFields || new Set();

    for (const field of indexedFields) {
      const value = data[field];

      if (value === undefined || value === null) {
        continue; // Skip undefined or null values
      }

      const tagValue = this.encodeTagValueForField(field, value);

      if (tagValue !== null) {
        tags[field] = tagValue;
      }
    }

    return tags;
  }

  encodeTagValueForField(field, value) {
    if (
      typeof value === 'string' ||
      typeof value === 'number' ||
      value instanceof Date
    ) {
      // Convert Date objects to ISO strings
      let tagValue =
        value instanceof Date ? value.toISOString() : value.toString();
  
      // For fields with potential invalid characters or unique constraints, encode or hash
      if (this.fieldRequiresHashing(field)) {
        // Hash the value
        tagValue = hashTagValue(tagValue);
      } else {
        // Use custom encoding to replace disallowed characters
        tagValue = encodeTagValue(tagValue);
      }
  
      return tagValue;
    } else {
      // For non-stringable types, skip tagging
      console.warn(
        `Field "${field}" has unsupported type for tagging and will be skipped.`
      );
      return null;
    }
  }

  fieldRequiresHashing(field) {
    // Define fields that require hashing
    const fieldsToHash = ['email', 'username'];

    return fieldsToHash.includes(field);
  }

  isValidTagValue(value) {
    // Azure Blob Storage tag value regex: ^[\w\s.-_/:]+$
    const regex = /^[\w\s.\-\/:]+$/;
    return regex.test(value);
  }

  async find(collection, query, options = {}) {
    const { limit = Infinity, offset = 0 } = options;
    const structuredQuery = parseQuery(query);
    const containerClient = await this.getContainerClient(collection);
  
    // Load index definitions
    const indexDefs = await this.loadIndexDefinitions(collection);
  
    // Convert the structured query to a tag filter SQL expression
    const tagFilterSqlExpression = this.convertQueryToTagFilter(structuredQuery, collection, indexDefs);
  
    let iterator;
    if (tagFilterSqlExpression) {
      iterator = containerClient.findBlobsByTags(tagFilterSqlExpression);
    } else {
      // If no query is provided or cannot be converted, list all blobs
      iterator = containerClient.listBlobsFlat();
    }
  
    const blobs = [];
    let skipped = 0;
  
    for await (const blob of iterator) {
      if (blob.name.startsWith('__')) {
        continue; // Skip system blobs
      }
  
      if (skipped < offset) {
        skipped++;
        continue;
      }
  
      const doc = await this.read(collection, blob.name);
      if (doc) {
        blobs.push(doc);
      }
  
      if (blobs.length >= limit) {
        break;
      }
    }
  
    return blobs;
  }

  async countDocuments(collection, query) {
    const structuredQuery = parseQuery(query);
    const containerClient = await this.getContainerClient(collection);
  
    // Load index definitions
    const indexDefs = await this.loadIndexDefinitions(collection);
  
    // Convert the structured query to a tag filter SQL expression
    const tagFilterSqlExpression = this.convertQueryToTagFilter(structuredQuery, collection, indexDefs);
  
    let iterator;
    if (tagFilterSqlExpression) {
      iterator = containerClient.findBlobsByTags(tagFilterSqlExpression);
    } else {
      // If no query is provided or cannot be converted, list all blobs
      iterator = containerClient.listBlobsFlat();
    }
  
    let count = 0;
  
    for await (const blob of iterator) {
      if (blob.name.startsWith('__')) {
        continue; // Skip system blobs
      }
      count++;
    }
  
    return count;
  }

  convertQueryToTagFilter(structuredQuery, collection, indexDefs) {
    if (Object.keys(structuredQuery).length === 0) {
      return null;
    }
  
    const conditions = [];
    const indexedFields = indexDefs.indexedFields || new Set();
  
    for (const [field, fieldConditions] of Object.entries(structuredQuery)) {
      if (!indexedFields.has(field)) {
        // Cannot use tag filters for fields that are not indexed
        return null;
      }
  
      if (Array.isArray(fieldConditions)) {
        // Multiple conditions for the same field
        for (const condition of fieldConditions) {
          const operatorCondition = operatorToTagCondition(field, condition, this);
          if (operatorCondition) {
            conditions.push(operatorCondition);
          } else {
            // Cannot represent this condition with blob index tags
            return null;
          }
        }
      } else {
        // Single condition for the field
        const operatorCondition = operatorToTagCondition(field, fieldConditions, this);
        if (operatorCondition) {
          conditions.push(operatorCondition);
        } else {
          // Cannot represent this condition with blob index tags
          return null;
        }
      }
    }
  
    return conditions.join(' AND ');
  }

  async listCollections() {
    const containers = this.blobServiceClient.listContainers();
    const containerNames = [];
    for await (const container of containers) {
      if (!container.name.startsWith('__')) {
        containerNames.push(container.name);
      }
    }
    return containerNames;
  }

  async dropCollection(collection) {
    const containerClient = await this.getContainerClient(collection);
    await containerClient.delete();

    // Remove index definitions cache
    delete this.indexDefinitions[collection];
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
