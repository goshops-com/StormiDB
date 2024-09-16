// src/storage/AzureBlobStorage.js

const { BlobServiceClient } = require('@azure/storage-blob');
const { parseQuery, operatorToTagCondition, Operator } = require('../query/QueryParser');
const { ulid } = require('ulid');
const { encodeTagValue, decodeTagValue, hashTagValue } = require('./tagEncoding');
const crypto = require('crypto');

const DEFAULT_RETRY_OPTIONS = {
  maxRetries: 5,
  initialDelay: 100, // milliseconds
  maxDelay: 5000, // milliseconds
};

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
    const retryOptions = { ...DEFAULT_RETRY_OPTIONS, ...options.retry };
    let attempt = 0;
  
    while (true) {
      try {
        if (!Array.isArray(fields)) {
          fields = [fields];
        }
  
        const { unique = false } = options;
  
        // Load existing index definitions
        const indexDefs = await this.loadIndexDefinitions(collection);
  
        // Create a compound index identifier
        const indexId = fields.join('_');
  
        if (indexDefs.indexes.has(indexId)) {
          // Update existing index
          const existingIndex = indexDefs.indexes.get(indexId);
          existingIndex.unique = unique;
        } else {
          // Add new index
          if (indexDefs.indexes.size >= 10) {
            throw new Error(`Cannot create more than 10 indexes per collection due to tag limit.`);
          }
          indexDefs.indexes.set(indexId, { fields, unique });
        }
  
        // Update individual field indexing information
        for (const field of fields) {
          indexDefs.indexedFields.add(field);
          if (unique) {
            indexDefs.uniqueFields.add(field);
          }
        }
  
        // Save updated index definitions
        await this.saveIndexDefinitions(collection, indexDefs);
        
        // If we reach here, the operation was successful
        return;
      } catch (error) {
        if (error.message.includes("Concurrent modification detected") && attempt < retryOptions.maxRetries) {
          attempt++;
          const delay = Math.min(retryOptions.initialDelay * Math.pow(2, attempt), retryOptions.maxDelay);
          console.log(`Retrying createIndex for collection "${collection}" after ${delay}ms (attempt ${attempt})`);
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          // If it's not a concurrency error or we've exceeded max retries, throw the error
          throw error;
        }
      }
    }
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
      indexDefs.indexes = new Map(Object.entries(indexDefs.indexes || {}));
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
          indexes: new Map(),
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
      indexes: Object.fromEntries(indexDefs.indexes),
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

  

  findUsableCompoundIndex(structuredQuery, indexDefs) {
    const queryFields = Object.keys(structuredQuery);
    let bestIndex = null;
    let maxMatchingFields = 0;
  
    for (const [indexId, indexInfo] of indexDefs.indexes) {
      const matchingFields = indexInfo.fields.filter(field => queryFields.includes(field));
      if (matchingFields.length > maxMatchingFields) {
        maxMatchingFields = matchingFields.length;
        bestIndex = indexInfo;
      }
    }
  
    return bestIndex;
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
  
    // Ensure tags are properly formatted for Azure Blob Storage
    const formattedTags = this.formatTags(tags);
  
    console.log('Creating document with tags:', formattedTags);
  
    await blobClient.upload(JSON.stringify(data), Buffer.byteLength(JSON.stringify(data)), {
      tags: formattedTags,
    });
  
    return id;
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
  
    // Ensure tags are properly formatted for Azure Blob Storage
    const formattedTags = this.formatTags(tags);
  
    console.log('Updating document with tags:', formattedTags);
  
    // Overwrite the blob with new data and tags
    await blobClient.upload(JSON.stringify(data), Buffer.byteLength(JSON.stringify(data)), {
      overwrite: true,
      tags: formattedTags,
    });
  }
  
  async find(collection, query, options = {}) {
    console.log(`\nQuery:`, JSON.stringify(query));
  
    const { limit = Infinity, offset = 0, batchSize = 100 } = options;
    const structuredQuery = parseQuery(query);
    const containerClient = await this.getContainerClient(collection);
  
    // Load index definitions
    const indexDefs = await this.loadIndexDefinitions(collection);
  
    // Convert the structured query to a tag filter SQL expression
    const tagFilterSqlExpression = this.convertQueryToTagFilter(structuredQuery, collection, indexDefs);
  
    console.log('Azure Blob Storage tag filter:', tagFilterSqlExpression);
  
    let results = [];
  
    if (Object.keys(structuredQuery).length === 0) {
      console.log('Empty query, paginating blobs');
      const iterator = containerClient.listBlobsFlat().byPage({ maxPageSize: batchSize });
      for await (const page of iterator) {
        for (const blob of page.segment.blobItems) {
          if (blob.name.startsWith('__')) continue;
          if (results.length >= offset + limit) break;
          const doc = await this.read(collection, blob.name);
          if (doc) results.push(doc);
        }
        if (results.length >= offset + limit) break;
      }
    } else if (tagFilterSqlExpression) {
      console.log('Using tag-based query');
      const iterator = containerClient.findBlobsByTags(tagFilterSqlExpression);
      for await (const blob of iterator) {
        if (blob.name.startsWith('__')) continue;
        console.log('Found blob:', blob.name);
        const doc = await this.read(collection, blob.name);
        if (doc) results.push(doc);
      }
    } else {
      console.log('No usable indexes, performing full scan with in-memory filtering');
      const iterator = containerClient.listBlobsFlat();
      for await (const blob of iterator) {
        if (blob.name.startsWith('__')) continue;
        const doc = await this.read(collection, blob.name);
        console.log('Checking document:', doc);
        if (doc && this.applyInMemoryFilter([doc], structuredQuery).length > 0) {
          console.log('Document matched:', doc);
          results.push(doc);
        }
      }
    }
  
    console.log(`Found ${results.length} documents before pagination`);
    
    // Apply offset and limit
    results = results.slice(offset, offset + limit);
  
    console.log(`Returning ${results.length} documents after pagination`);
    return results;
  }
  
  formatTags(tags) {
    return Object.fromEntries(
      Object.entries(tags).map(([key, value]) => [key, value.toString()])
    );
  }
  
  convertQueryToTagFilter(structuredQuery, collection, indexDefs) {
    if (Object.keys(structuredQuery).length === 0) {
      return null;
    }
  
    const conditions = [];
    const indexedFields = indexDefs.indexedFields || new Set();
  
    for (const [field, fieldConditions] of Object.entries(structuredQuery)) {
      if (!indexedFields.has(field)) {
        console.log(`Field ${field} is not indexed, skipping for tag filter`);
        continue;
      }
  
      const conditionArray = Array.isArray(fieldConditions) ? fieldConditions : [fieldConditions];
      for (const condition of conditionArray) {
        const operatorCondition = operatorToTagCondition(field, condition, this);
        if (operatorCondition) {
          conditions.push(operatorCondition);
        } else {
          console.log(`Could not create tag condition for field ${field}:`, condition);
        }
      }
    }
  
    return conditions.length > 0 ? conditions.join(' AND ') : null;
  }

  applyInMemoryFilter(docs, structuredQuery) {
    return docs.filter(doc => {
      for (const [field, conditions] of Object.entries(structuredQuery)) {
        const conditionArray = Array.isArray(conditions) ? conditions : [conditions];
        
        for (const condition of conditionArray) {
          const { operator, value } = condition;
          const docValue = doc[field];

          switch (operator) {
            case Operator.EQ:
              if (docValue !== value) return false;
              break;
            case Operator.GT:
              if (docValue <= value) return false;
              break;
            case Operator.LT:
              if (docValue >= value) return false;
              break;
            case Operator.GTE:
              if (docValue < value) return false;
              break;
            case Operator.LTE:
              if (docValue > value) return false;
              break;
            case Operator.IN:
              if (!Array.isArray(value) || !value.includes(docValue)) return false;
              break;
            case Operator.NIN:
              if (Array.isArray(value) && value.includes(docValue)) return false;
              break;
            case Operator.BETWEEN:
              if (!Array.isArray(value) || value.length !== 2 || docValue < value[0] || docValue > value[1]) return false;
              break;
            default:
              console.log(`Unsupported operator ${operator} for in-memory filtering`);
              return false;
          }
        }
      }
      return true;
    });
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
