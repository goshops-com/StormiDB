// src/StormiDB.js

const { query } = require("express");

class StormiDB {
  constructor(storage) {
    this.storage = storage;
  }

  async create(collection, data, id = null) {
    return this.storage.create(collection,data);
  }

  async find(collection, query, options = {}) {
    return this.storage.find(collection, query, options);
  }

  async findById(collection, id) {
    return this.storage.read(collection, id);
  }
  
  async findOne(collection, query) {
    const results = await this.storage.find(collection, query, { limit: 1 });
    return results[0] || null;
  }

  async update(collection, id, data) {
    return this.storage.update(collection, id, data);
  }

  async delete(collection, id) {
    return this.storage.delete(collection, id);
  }

  async createIndex(collection, field, options = {}) {
    return this.storage.createIndex(collection, field, options);
  }

  async dropCollection(collection) {
    return this.storage.dropCollection(collection);
  }

  async getCollections() {
    return this.storage.listCollections();
  }

  async countDocuments(collection, query = {}){
    return this.storage.countDocuments(collection, query)
  }
}

module.exports = StormiDB;
