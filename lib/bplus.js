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

BPlus.prototype.get = function get(key, callback) {
  if (typeof key === 'string') key = new Buffer(key);

  return this._db.get(key, function(err, result) {
    if (!callback) return;
    if (err) return callback(err, result);
    return callback(err, result.value, result.ref);
  });
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
