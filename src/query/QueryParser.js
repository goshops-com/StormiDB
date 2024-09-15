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

const operatorFunctions = {
  [Operator.EQ]: (a, b) => a === b,
  [Operator.GT]: (a, b) => a > b,
  [Operator.LT]: (a, b) => a < b,
  [Operator.GTE]: (a, b) => a >= b,
  [Operator.LTE]: (a, b) => a <= b,
  [Operator.IN]: (a, b) => Array.isArray(b) && b.includes(a),
  [Operator.NIN]: (a, b) => Array.isArray(b) && !b.includes(a),
  [Operator.BETWEEN]: (a, b) => a >= b[0] && a <= b[1],
  // Add more operator functions as needed
};

function parseQuery(query) {
  const structuredQuery = {};

  for (const [field, condition] of Object.entries(query)) {
    if (typeof condition === 'object' && condition !== null) {
      const parsedCondition = {
        operator: Operator.EQ,
        value: condition,
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
        value: condition,
      };
    }
  }

  return structuredQuery;
}

function evaluateCondition(doc, field, condition) {
  const docValue = doc[field];

  // Handle cases where the field doesn't exist in the document
  if (docValue === undefined) {
    return false;
  }

  let docValueToCompare = docValue;
  let conditionValueToCompare = condition.value;

  // Check if docValue is an ISO date string
  if (isISODateString(docValue)) {
    // Convert docValue to timestamp
    docValueToCompare = new Date(docValue).getTime();

    // Handle condition values for different operators
    if (Array.isArray(conditionValueToCompare)) {
      conditionValueToCompare = conditionValueToCompare.map(value => {
        return isISODateString(value) ? new Date(value).getTime() : value;
      });
    } else {
      if (isISODateString(conditionValueToCompare)) {
        conditionValueToCompare = new Date(conditionValueToCompare).getTime();
      }
    }
  }

  const comparisonFunction = operatorFunctions[condition.operator];
  return comparisonFunction(docValueToCompare, conditionValueToCompare);
}

// Helper function to check if a string is an ISO date string
function isISODateString(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/.test(value);
}

module.exports = {
  Operator,
  operatorFunctions,
  parseQuery,
  evaluateCondition,
};
