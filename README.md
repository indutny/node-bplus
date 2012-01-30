# Node.js bindings for a [bplus](https://github.com/indutny/bplus) C libarary [![Build Status](https://secure.travis-ci.org/indutny/node-bplus.png)](http://travis-ci.org/indutny/node-bplus)

# ** Danger zone! Possible unicorns and rainbows! **

Both database engine and node.js bindings are in development now, use carefully.

## Installation

```bash
npm install bplus
```

## Usage

```javascript
var bplus = require('bplus');


var db = bplus.create().open('/tmp/1.bp');

db.set('key', 'value', function(err) {
  db.get('key', function(err, value) {
    console.log('voila! Key is in database: %s', value);
    db.close();
  });
});
```

## API

### Basics

#### db = bplus.create()

Returns instance of database wrapper.

#### db.open(filename)

Associates database with file until `.close()` will be called.

#### db.close()

Closes database.

#### db.set('key', 'value', [callback])

Stores key/value pair in database. Both `key` and `value` arguments may be
instances of `Buffer`. If write will fail, callback will receive `err` as the
first argument.

**Note:** If `key` is already in database - previous value will be
unconditionally overwritten.

#### db.get('key', [callback])

Searches database for `key` and invokes callback with it's value (or error).
`key` may be a `Buffer` or a `String`.

**Note:** Callback will also receive `ref` as third argument, see MVCC part
below for details.

#### db.remove('key', [callback])

Removes `key` from database or invokes callback with error if operation has
failed (callback will be called anyway upon completion).

#### db.compact([callback])

Runs compaction on database. Will significantly reduce database size and
increase speed of lookup and insertion.

**Note:** For big databases may take some time (~ 30 sec for a database with
1000000 records).

### MVCC

#### db.update('key', 'value', filter, [callback])

Same as `.set()`, but if `key` is already in database -
`filter(previousValue, currentValue)` will be called. Value will be replaced
only if return value of `filter` is not `false`.

#### db.removev('key', [filter], [callback]);

Same as `.remove()`, but `filter` is called before removing actual value. Should
be used to ensure that only specific value will be removed.

#### db.getPrevious(ref, [callback])

Returns previous value of `key`. `callback` will receive three arguments
(as in `.get()`).

**Note:** That after compaction

### Streaming

#### db.getRange('start', 'end', [filter])

Returns a `promise` object that'll emit `message` event for each key/value pair
found in specified range. `end` will be emitted on stream finish.

```javascript
var promise = db.getRange('start', 'end');

promise.on('message', function(key, value, ref) {
  // see `.get()` for details
});

promise.on('end', function() {
  // Ended
});
```

`filter` can be passed to load values only of specific keys. Value won't be
loaded (and emitted in `message` event) only if filter returned `false` for it.

```javascript
db.getRange('start', 'end', function(key) {
  return key.toString() === 'somekey';
});
```

#### db.bulk(keyValues, [callback])

Inserts multiple key/values in one atomic operation.

```javascript
var kvs = [
  {
    key: 'a',
    value: 'a'
  },
  {
    key: 'b',
    value: 'b'
  }
];
db.bulk(kvs, function(err) {
  // .. your code ..
});
```

### db.bulkUpdate(keyValues, filter, [callback])

Same as `.bulk`, but with `filter` as in `.update()`.

#### LICENSE

This software is licensed under the MIT License.

Copyright Fedor Indutny, 2012.

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the
following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
USE OR OTHER DEALINGS IN THE SOFTWARE.
