// Export utils
exports.utils = require('./bplus/utils');

// Export binding
try {
  exports.binding = require('./bplus/bplus');
} catch (e) {
  throw Error('BPlus can\' be loaded without compiled addon');
}

// Export Bplus constructor (core)
exports.BPlus = require('./bplus/core').BPlus;
exports.create = require('./bplus/core').create;
