// src/query/QueryParser.js

// src/query/QueryParser.js

function operatorToTagCondition(field, condition, storageInstance) {
  console.log('Field:', field, 'Condition:', JSON.stringify(condition));

  if (!condition || typeof condition !== 'object') {
    console.log(`Invalid condition for field ${field}:`, condition);
    return null;
  }

  // If condition is a direct value (e.g., { city: "New York" })
  if (!condition.operator) {
    condition = { operator: Operator.EQ, value: condition };
  }

  const operator = operatorToTagOperator[condition.operator];

  if (!operator) {
    console.log(`Unsupported operator for field ${field}:`, condition.operator);
    return null;
  }

  // Use storageInstance.encodeTagValueForField to ensure consistent encoding/hashing
  const encodeValue = (val) => {
    return storageInstance.encodeTagValueForField(field, val);
  };

  let value = condition.value;

  switch (condition.operator) {
    case Operator.EQ:
      return `"${field}" = '${encodeValue(value)}'`;
    case Operator.GT:
      return `"${field}" > '${encodeValue(value)}'`;
    case Operator.LT:
      return `"${field}" < '${encodeValue(value)}'`;
    case Operator.GTE:
      return `"${field}" >= '${encodeValue(value)}'`;
    case Operator.LTE:
      return `"${field}" <= '${encodeValue(value)}'`;
    case Operator.BETWEEN:
      if (!Array.isArray(value) || value.length !== 2) {
        console.log(`Invalid value for BETWEEN operator on field ${field}:`, value);
        return null;
      }
      return `"${field}" > '${encodeValue(value[0])}' AND "${field}" < '${encodeValue(value[1])}'`;
    default:
      console.log(`Unsupported operator ${condition.operator} for field ${field}`);
      return null;
  }
}

// Remove IN and NIN from Operator and operatorMap
const Operator = {
  EQ: 'EQ',
  GT: 'GT',
  LT: 'LT',
  GTE: 'GTE',
  LTE: 'LTE',
  BETWEEN: 'BETWEEN',
};

const operatorMap = {
  $eq: Operator.EQ,
  $gt: Operator.GT,
  $lt: Operator.LT,
  $gte: Operator.GTE,
  $lte: Operator.LTE,
  $between: Operator.BETWEEN,
};

module.exports = {
  Operator,
  parseQuery,
  operatorToTagCondition,
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



module.exports = {
  Operator,
  parseQuery,
  operatorToTagCondition,
};