var Buffer = require('buffer').Buffer,
    binding;

try {
  binding = require('./bplus/bplus');
} catch (e) {
  throw Error('BPlus can\' be loaded without compiled addon');
}

function BPlus() {
  this._db = new binding.BPlus();
};
exports.BPlus = BPlus;

exports.create = function() {
  return new BPlus();
};

BPlus.prototype.open = function open(filename) {
  return this._db.open(filename);
};

BPlus.prototype.close = function close() {
  return this._db.close();
};

BPlus.prototype.set = function set(key, value, callback) {
  callback || (callback = function() {});

  if (typeof key === 'string') key = new Buffer(key);
  if (typeof value === 'string') value = new Buffer(value);

  return this._db.set(key, value, callback);
};

BPlus.prototype.bulk = function bulk(keyValues, callback) {
  callback || (callback = function() {});
  if (!Array.isArray(keyValues)) {
    return callback(Error('First argument should be an Array'));
  }

  var binaryKvs = keyValues.map(function(kv) {
    return {
      key: typeof kv.key === 'string' ? new Buffer(kv.key) : kv.key,
      value: typeof kv.value === 'string' ? new Buffer(kv.value) : kv.value
    };
  });

  return this._db.bulkSet(binaryKvs, callback);
};

BPlus.prototype.get = function get(key, callback) {
  if (typeof key === 'string') key = new Buffer(key);

  return this._db.get(key, function(err, result) {
    if (!callback) return;
    if (err) return callback(err, result);
    return callback(err, result.value, result.ref);
  });
};

BPlus.prototype.getRange = function getRange(start, end, filter) {
  var promise = new process.EventEmitter;

  if (typeof start === 'string') start = new Buffer(start);
  if (typeof end === 'string') end = new Buffer(end);

  function callback(err, type, key, value) {
    if (err) return promise.emit('error', err, type);

    if (type === 'message') {
      promise.emit('message', key.value, value.value, value.ref);
    } else {
      promise.emit('end');
    }
  }

  if (filter) {
    var self = this;
    process.nextTick(function() {
      self._db.getFilteredRange(start, end, function(key) {
        return filter(key.value)
      }, callback);
    });
  } else {
    this._db.getRange(start, end, callback);
  }

  return promise;
};

BPlus.prototype.getPrevious = function getPrevious(ref, callback) {
  return this._db.getPrevious(ref, function(err, result) {
    if (!callback) return;
    if (err) return callback(err, result);
    return callback(err, result.value, result.ref);
  });
};

BPlus.prototype.remove = function remove(key, callback) {
  callback || (callback = function() {});

  if (typeof key === 'string') key = new Buffer(key);

  return this._db.remove(key, callback);
};

BPlus.prototype.compact = function compact(callback) {
  callback || (callback = function() {});

  return this._db.compact(callback);
};
