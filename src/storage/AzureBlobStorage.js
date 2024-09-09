// src/storage/AzureBlobStorage.js

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
  }

  sanitizeContainerName(name) {
    name = `${this.prefix}${name}`;

    // Stricter sanitization: only lowercase letters, numbers, and single hyphens
    let sanitized = name.toLowerCase().replace(/[^a-z0-9-]/g, '');
    sanitized = sanitized.replace(/-+/g, '-');  // Replace multiple hyphens with single hyphen
    sanitized = sanitized.replace(/^-|-$/g, ''); // Remove leading and trailing hyphens
    sanitized = sanitized.substring(0, 63); // Ensure name is not longer than 63 characters
    if (sanitized.length < 3) {
      sanitized = sanitized.padEnd(3, 'a');  // Ensure name is at least 3 characters
    }
    return sanitized;
  }

  sanitizeBlobName(name) {
    // Replace invalid characters, but keep case
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
    const containerClient = await this.getContainerClient(collection);
    const blobClient = containerClient.getBlockBlobClient(id);
    await blobClient.upload(JSON.stringify(data), JSON.stringify(data).length);
    await this.updateIndexes(collection, id, data);
    return id;
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
    await this.create(collection, id, data);
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
    const indexedField = await this.findBestIndex(collection, structuredQuery);

    let candidateDocuments;
    if (indexedField) {
      candidateDocuments = await this.lookupIndex(collection, indexedField, structuredQuery[indexedField]);
    } else {
      candidateDocuments = await this.fullCollectionScan(collection);
    }

    const filteredDocuments = candidateDocuments.filter(doc =>
      Object.entries(structuredQuery).every(([field, condition]) =>
        evaluateCondition(doc, field, condition)
      )
    );

    return filteredDocuments.slice(offset, offset + limit);
  }

  async createIndex(collection, field) {
    const index = {};
    const containerClient = await this.getContainerClient(collection);
    const blobs = containerClient.listBlobsFlat();

    for await (const blob of blobs) {
      const doc = await this.read(collection, blob.name);
      const value = doc[field];
      if (value !== undefined) {
        if (!index[value]) {
          index[value] = [];
        }
        index[value].push(blob.name);
      }
    }

    const indexBlobClient = this.indexContainer.getBlockBlobClient(`${collection}_${field}`);
    await indexBlobClient.upload(JSON.stringify(index), JSON.stringify(index).length);
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
      const field = indexBlob.name.split('_')[1];
      const index = await this.getIndex(collection, field);
      if (index) {
        if (oldData) {
          const oldValue = oldData[field];
          if (oldValue !== undefined) {
            index[oldValue] = index[oldValue].filter(docId => docId !== id);
            if (index[oldValue].length === 0) {
              delete index[oldValue];
            }
          }
        }
        const newValue = newData[field];
        if (newValue !== undefined) {
          if (!index[newValue]) {
            index[newValue] = [];
          }
          if (!index[newValue].includes(id)) {
            index[newValue].push(id);
          }
        }
        await this.indexContainer.getBlockBlobClient(indexBlob.name).upload(JSON.stringify(index), JSON.stringify(index).length);
      }
    }
  }

  async removeFromIndexes(collection, id, oldData) {
    const indexBlobs = this.indexContainer.listBlobsFlat({ prefix: `${collection}_` });
    for await (const indexBlob of indexBlobs) {
      const field = indexBlob.name.split('_')[1];
      const index = await this.getIndex(collection, field);
      if (index) {
        const oldValue = oldData[field];
        if (oldValue !== undefined && index[oldValue]) {
          index[oldValue] = index[oldValue].filter(docId => docId !== id);
          if (index[oldValue].length === 0) {
            delete index[oldValue];
          }
          await this.indexContainer.getBlockBlobClient(indexBlob.name).upload(JSON.stringify(index), JSON.stringify(index).length);
        }
      }
    }
  }

  async findBestIndex(collection, structuredQuery) {
    const indexBlobs = this.indexContainer.listBlobsFlat({ prefix: `${collection}_` });
    for await (const indexBlob of indexBlobs) {
      const field = indexBlob.name.split('_')[1];
      if (field in structuredQuery) {
        return field;
      }
    }
    return null;
  }

  async lookupIndex(collection, field, condition) {
    const index = await this.getIndex(collection, field);
    if (!index) return [];

    let matchingIds = [];
    if (condition.operator === 'EQ') {
      matchingIds = index[condition.value] || [];
    } else {
      for (const [value, ids] of Object.entries(index)) {
        if (evaluateCondition({ [field]: value }, field, condition)) {
          matchingIds.push(...ids);
        }
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