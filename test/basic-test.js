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
  });

  test('should be to set and get key/value', function(done) {
    db.set('key', 'value', function(err) {
      assert.ok(!err);
      db.get('key', function(err, value) {
        assert.ok(!err);
        assert.equal(value.toString(), 'value');
        done();
      });
    });
  });
});
