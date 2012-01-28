var bplus = require('../bplus'),
    Buffer = require('buffer').Buffer;

var utils = exports;

//
// ### function toBuffer (value)
// #### @value {String|Buffer} source value
// If value is string - converts it to buffer, otherwise returns value
//
utils.toBuffer = function toBuffer(value) {
  return typeof value === 'string' ? new Buffer(value) : value;
};

//
// ### function arrayToBuffer (arr)
// #### @arr {Array} source array
// Converts array entities' keys/values to buffers
//
utils.arrayToBuffer = function arrayToBuffer(arr) {
  return arr.map(function(kv) {
    return {
      key: utils.toBuffer(kv.key),
      value: utils.toBuffer(kv.value)
    };
  });
};

//
// ### function passSyncResult (callback, result)
// #### @callback {Function} continuation
// #### @result {True|Number} sync action result
// Executes callback in next tick with arguments corresponding t
// `result`'s value
//
utils.passSyncResult = function passSyncResult(callback, result) {
  process.nextTick(function() {
    if (result === true) {
      callback(null);
    } else {
      callback(true, result);
    }
  });
};
