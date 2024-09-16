// src/storage/tagEncoding.js

function encodeTagValue(value) {
  // Escape underscores
  let encoded = value.replace(/_/g, '__');

  // Replace disallowed characters
  encoded = encoded.replace(/[^a-zA-Z0-9\s.\-\/:]/g, (char) => {
    const hexCode = char.charCodeAt(0).toString(16).toUpperCase().padStart(2, '0');
    return `_${hexCode}`;
  });

  return encoded;
}

function decodeTagValue(encodedValue) {
  // Replace encoded characters
  let decoded = encodedValue.replace(/_([0-9A-F]{2})/g, (match, hex) => {
    return String.fromCharCode(parseInt(hex, 16));
  });

  // Convert double underscores back to single underscores
  decoded = decoded.replace(/__/g, '_');

  return decoded;
}

function hashTagValue(value) {
  // Hash the value using SHA-256
  return require('crypto').createHash('sha256').update(value).digest('hex');
}

module.exports = { encodeTagValue, decodeTagValue, hashTagValue };
