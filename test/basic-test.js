var assert = require('assert'),
    fs = require('fs'),
    bplus = require('..');

suite('BPlus addon', function() {
  var db;
  setup(function() {
    try {
      fs.unlinkSync('/tmp/test.bp');
    } catch (e) {
    }
    db = bplus.create().open('/tmp/test.bp');
  });

  teardown(function() {
    db.close();
    try {
      fs.unlinkSync('/tmp/test.bp');
    } catch (e) {
    }
  });

  test('should not found not inserted value', function(done) {
    db.get('key', function(err) {
      assert.ok(err);
      done();
    });
  });

  test('should set and get key/value', function(done) {
    db.set('key', 'value', function(err) {
      assert.ok(!err);
      db.get('key', function(err, value) {
        assert.ok(!err);
        assert.equal(value.toString(), 'value');
        done();
      });
    });
  });

  test('should set, remove and not found key after', function(done) {
    db.set('key', 'value', function(err) {
      assert.ok(!err);
      db.remove('key', function(err) {
        assert.ok(!err);
        db.get('key', function(err, value) {
          assert.ok(err);
          assert.ok(typeof value === 'number');
          assert.ok(value > 0);
          done();
        });
      });
    });
  });

  test('should set, compact, set and get both values', function(done) {
    db.set('key1', 'value1', function(err) {
      assert.ok(!err);
      db.compact(function(err) {
        assert.ok(!err);
        db.set('key2', 'value2', function(err) {
          assert.ok(!err);
          db.get('key1', function(err, value) {
            assert.ok(!err);
            assert.equal(value.toString(), 'value1');
            db.get('key2', function(err, value) {
              assert.ok(!err);
              assert.equal(value.toString(), 'value2');
              done();
            });
          });
        });
      });
    });
  });

  test('should set, overwrite and get previous', function(done) {
    db.set('k', 'v1', function(err) {
      assert.ok(!err);
      db.set('k', 'v2', function(err) {
        assert.ok(!err);
        db.get('k', function(err, value, ref) {
          assert.ok(!err);
          assert.equal(value.toString(), 'v2');
          db.getPrevious(ref, function(err, value) {
            assert.ok(!err);
            assert.equal(value.toString(), 'v1');
            done();
          });
        });
      });
    });
  });

  test('should return correct key/values on .getRange()', function(done) {
    db.set('k1', 'v1', function(err) {
      assert.ok(!err);
      db.set('k2', 'v2', function(err) {
        assert.ok(!err);
        db.set('k3', 'v3', function(err) {
          assert.ok(!err);
          var matched = 0;
          db.getRange('k2', 'k3').on('message', function(key, value) {
            assert.ok(key.toString() === 'k2' || key.toString() === 'k3');
            assert.ok(value.toString() === 'v2' || value.toString() === 'v3');
            matched++;
          }).on('end', function() {
            assert.equal(matched, 2);
            done();
          });
        });
      });
    });
  });

  test('should return correct results on .getRange(filter)', function(done) {
    db.set('k1', 'v1', function(err) {
      assert.ok(!err);
      db.set('k2', 'v2', function(err) {
        assert.ok(!err);
        db.set('k3', 'v3', function(err) {
          assert.ok(!err);
          var matched = 0;

          function filter(key) {
            return key.toString() !== 'k1';
          }

          db.getRange('k1', 'k3', filter).on('message', function(key, value) {
            assert.ok(key.toString() === 'k2' || key.toString() === 'k3');
            assert.ok(value.toString() === 'v2' || value.toString() === 'v3');
            matched++;
          }).on('end', function() {
            assert.equal(matched, 2);
            done();
          });
        });
      });
    });
  });

  test('should insert kvs in bulk', function(done) {
    var kvs = [
      { key: '1', value: '1' },
      { key: '2', value: '2' },
      { key: '3', value: '3' },
      { key: '4', value: '4' }
    ];

    db.bulk(kvs, function(err) {
      assert.ok(!err);
      db.get('2', function(err, value) {
        assert.ok(!err);
        assert.equal(value.toString(), '2');
        done();
      });
    });
  });
});
