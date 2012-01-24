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
    db = bplus.create();
    db.open('/tmp/test.bp');
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
});
