// src/query/QueryParser.js

const Operator = {
  EQ: 'EQ',
  GT: 'GT',
  LT: 'LT',
  GTE: 'GTE',
  LTE: 'LTE',
  IN: 'IN',
  NIN: 'NIN',
  BETWEEN: 'BETWEEN',
  // Add more operators as needed
};

const operatorMap = {
  $eq: Operator.EQ,
  $gt: Operator.GT,
  $lt: Operator.LT,
  $gte: Operator.GTE,
  $lte: Operator.LTE,
  $in: Operator.IN,
  $nin: Operator.NIN,
  $between: Operator.BETWEEN,
  // Add more mappings as needed
};

const operatorToTagOperator = {
  [Operator.EQ]: '=',
  [Operator.GT]: '>',
  [Operator.LT]: '<',
  [Operator.GTE]: '>=',
  [Operator.LTE]: '<=',
  [Operator.IN]: 'IN',
  [Operator.BETWEEN]: 'BETWEEN',
  // Note: Azure Blob Storage doesn't support NIN directly
};

const { encodeTagValue, hashTagValue } = require('../storage/tagEncoding');

function parseQuery(query) {
  const structuredQuery = {};

  for (const [field, condition] of Object.entries(query)) {
    if (typeof condition === 'object' && condition !== null && !Array.isArray(condition)) {
      const parsedConditions = [];

      for (const [op, value] of Object.entries(condition)) {
        if (op in operatorMap) {
          parsedConditions.push({
            operator: operatorMap[op],
            value: value,
          });
        }
      }

      if (parsedConditions.length > 0) {
        structuredQuery[field] = parsedConditions;
      } else {
        structuredQuery[field] = {
          operator: Operator.EQ,
          value: condition,
        };
      }
    } else {
      structuredQuery[field] = {
        operator: Operator.EQ,
        value: condition,
      };
    }
  }

  return structuredQuery;
}

function operatorToTagCondition(field, condition, storageInstance) {
  const operator = operatorToTagOperator[condition.operator];

  if (!operator) {
    // Operator not supported for tag queries
    return null;
  }

  // Use storageInstance.encodeTagValueForField to ensure consistent encoding/hashing
  const encodeValue = (val) => {
    return storageInstance.encodeTagValueForField(field, val);
  };

  // Escape value for SQL expression
  const escapeValue = (val) => {
    return `'${val.replace(/'/g, "''")}'`;
  };

  let value = condition.value;

  if (condition.operator === Operator.IN) {
    if (!Array.isArray(value)) {
      return null;
    }
    const values = value
      .map((val) => escapeValue(encodeValue(val)))
      .join(', ');
    return `\"${field}\" IN (${values})`;
  } else if (condition.operator === Operator.BETWEEN) {
    if (!Array.isArray(value) || value.length !== 2) {
      return null;
    }
    const [start, end] = value.map((val) => escapeValue(encodeValue(val)));
    return `\"${field}\" BETWEEN ${start} AND ${end}`;
  } else {
    return `\"${field}\" ${operator} ${escapeValue(encodeValue(value))}`;
  }
}

module.exports = {
  Operator,
  parseQuery,
  operatorToTagCondition,
};