var bplus = require('../bplus'),
    utils = bplus.utils;

var core = exports;

//
// ### function BPlus ()
// BPlus database constructor
//
function BPlus() {
  this._db = new bplus.binding.BPlus();
};
core.BPlus = BPlus;

//
// ### function create ()
// BPlus database constructor wrapper
//
core.create = function create() {
  return new BPlus();
};

//
// ### function open (filename)
// #### @filename {String} path to database file
// Opens database
//
BPlus.prototype.open = function open(filename) {
  this._db.open(filename);

  return this;
};

//
// ### function close ()
// Closes database
//
BPlus.prototype.close = function close() {
  this._db.close();

  return this;
};

//
// ### function set (key, value, callback)
// #### @key {String|Buffer} key
// #### @value {String|Buffer} value
// #### @callback {Function} continuation
// Inserts key-value pair into database
// (NOTE: if key already in database, value will be overwritten)
//
BPlus.prototype.set = function set(key, value, callback) {
  callback || (callback = function() {});

  key = utils.toBuffer(key);
  value = utils.toBuffer(value);

  this._db.set(key, value, callback);

  return this;
};

//
// ### function update (key, value, filter, callback)
// #### @key {String|Buffer} key
// #### @value {String|Buffer} value
// #### @filter {Function} resolve conflict callback
// #### @callback {Function} continuation
// Inserts or updates key-value pair into database
//
// If key is already in database - `filter(prev_value, curr_value)` will be
// called and if result is not `false` entry will be updated.
//
BPlus.prototype.update = function update(key, value, filter, callback) {
  callback || (callback = function() {});
  filter || (filter = function() {});

  key = utils.toBuffer(key);
  value = utils.toBuffer(value);

  var res = this._db.update(key, value, function(prev, curr) {
    return filter(prev.value, curr.value);
  });

  utils.passSyncResult(callback, res);

  return this;
};

//
// ### function bulk (keyValues, callback)
// #### @keyValues {Array} key/value pairs to set in db
// #### @callback {Function} continuation
// Inserts multiple key/value pairs into database
// (NOTE: if key already in database, value will be overwritten)
//
BPlus.prototype.bulk = function bulk(keyValues, callback) {
  callback || (callback = function() {});

  this._db.bulkSet(utils.arrayToBuffer(keyValues), callback);

  return this;
};

//
// ### function bulkUpdate (keyValues, filter, callback)
// #### @keyValues {Array} key/value pairs to set in db
// #### @filter {Function} resolve conflict callback
// #### @callback {Function} continuation
// Inserts or updates key/value pairs into database
//
// If key is already in database - `filter(prev_value, curr_value)` will be
// called and if result is not `false` entry will be updated.
//
BPlus.prototype.bulkUpdate = function bulkUpdate(keyValues, filter, callback) {
  filter || (filter = function() {});
  callback || (callback = function() {});

  var res = this._db.bulkUpdate(
      utils.arrayToBuffer(keyValues),
      function(prev, curr) {
        return filter(prev.value, curr.value);
      }
  );

  utils.passSyncResult(callback, res);

  return this;
};

//
// ### function get (key, callback)
// #### @key {String|Buffer} key
// #### @callback {Function} continuation
// Searches for key in db and calls callback with error or value.
// `callback` will be invoked with three arguments: (err, value, ref)
// `ref` can be used in `.getPrevious()`
//
BPlus.prototype.get = function get(key, callback) {
  callback || (callback = function() {});
  this._db.get(utils.toBuffer(key), function(err, result) {
    if (err) return callback(err, result);
    return callback(err, result.value, result.ref);
  });

  return this;
};

//
// ### function getRange (start, end, filter)
// #### @start {String|Buffer} start key
// #### @end {String|Buffer} end key
// #### @filter {Function} (optional) key filter
// Returns a `promise` object that will emit:
//  * ('message', key, value, ref) - for every matched key/value in range
//  * ('error') - on any error
//  * ('end') - once matching finished
//
// If filter callback is provided it'll be invoked with a `key` argument, and
// if result is not `false` - 'message' event with that key will be emitted.
//
BPlus.prototype.getRange = function getRange(start, end, filter) {
  var promise = new process.EventEmitter;

  start = utils.toBuffer(start);
  end = utils.toBuffer(end);

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

//
// ### function getPrevious (ref, callback)
// #### @ref {Buffer} `ref` returned by calling `.get()`
// #### @callback {Function} continuation
// Calls `callback` with (err, previous_value, previous_ref), where
// `previous_value` is a previous value of key, and
// `previous_ref` is a reference to previous-previous value
//
BPlus.prototype.getPrevious = function getPrevious(ref, callback) {
  this._db.getPrevious(ref, function(err, result) {
    if (!callback) return;
    if (err) return callback(err, result);
    return callback(err, result.value, result.ref);
  });

  return this;
};

//
// ### function remove (key, callback)
// #### @key {String|Buffer} key
// #### @callback {Function} continuation
// Searches for key and removes it from database
//
BPlus.prototype.remove = function remove(key, callback) {
  callback || (callback = function() {});

  key = utils.toBuffer(key);

  this._db.remove(key, callback);

  return this;
};


//
// ### function removev (key, filter, callback)
// #### @key {String|Buffer} key
// #### @filter {Function} resolve conflict callback
// #### @callback {Function} continuation
// Searches for key and removes it from database.
// `filter(value)` will be called and if result is not `false` - value will be
// removed from the database.
//
BPlus.prototype.removev = function removev(key, filter, callback) {
  callback || (callback = function() {});
  filter || (filter = function() {});

  if (typeof key === 'string') key = new Buffer(key);

  var res = this._db.removev(key, function(value) {
    return filter(value.value);
  }, callback);

  process.nextTick(function() {
    if (res === true) {
      callback(null);
    } else {
      callback(true, res);
    }
  });

  return this;
};

//
// ### function compact (callback)
// #### @callback {Function} continuation
// Runs compaction of database
//
BPlus.prototype.compact = function compact(callback) {
  callback || (callback = function() {});

  this._db.compact(callback);
  return this;
};
