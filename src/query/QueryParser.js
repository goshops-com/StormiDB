// src/query/QueryParser.js

const Operator = {
  EQ: 'EQ',
  GT: 'GT',
  LT: 'LT',
  GTE: 'GTE',
  LTE: 'LTE',
  IN: 'IN',
  NIN: 'NIN',
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
  // Add more mappings as needed
};

const operatorFunctions = {
  [Operator.EQ]: (a, b) => a === b,
  [Operator.GT]: (a, b) => a > b,
  [Operator.LT]: (a, b) => a < b,
  [Operator.GTE]: (a, b) => a >= b,
  [Operator.LTE]: (a, b) => a <= b,
  [Operator.IN]: (a, b) => Array.isArray(b) && b.includes(a),
  [Operator.NIN]: (a, b) => Array.isArray(b) && !b.includes(a),
  // Add more operator functions as needed
};

function parseQuery(query) {
  const structuredQuery = {};

  for (const [field, condition] of Object.entries(query)) {
    if (typeof condition === 'object' && condition !== null) {
      const parsedCondition = {
        operator: Operator.EQ,
        value: condition
      };

      for (const [op, value] of Object.entries(condition)) {
        if (op in operatorMap) {
          parsedCondition.operator = operatorMap[op];
          parsedCondition.value = value;
          break;
        }
      }

      structuredQuery[field] = parsedCondition;
    } else {
      structuredQuery[field] = {
        operator: Operator.EQ,
        value: condition
      };
    }
  }

  return structuredQuery;
}

function evaluateCondition(doc, field, condition) {
  const value = doc[field];
  const comparisonFunction = operatorFunctions[condition.operator];
  return comparisonFunction(value, condition.value);
}

module.exports = {
  Operator,
  operatorFunctions,
  parseQuery,
  evaluateCondition
};