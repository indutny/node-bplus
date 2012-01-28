var crypto = require('crypto'),
    marked = require('marked');

function sha1(value) {
  return crypto.createHash('sha1').update(value).digest('base64');
}

function Marks(db) {
  this.db = db;
};

Marks.prototype.get = function get(req, res, next) {
  var type = /\/edit$/.test(req.url) ? 'edit' : 'show',
      url = type === 'edit' ? req.url.replace(/\/edit$/, '') : req.url;

  this.db.get(url, function(err, value) {
    if (err) return next();

    var mark = JSON.parse(value.toString());

    // Sanitize JSON
    delete mark.password;

    res.render(type, {
      title: mark.title,
      content: marked(mark.body),
      mark: mark
    });
  });
};

Marks.prototype.set = function set(req, res) {
  var mark = {
    version: 1,
    url: req.url,
    password: sha1(req.body.password || ''),
    title: req.body.title,
    body: req.body.body
  };

  this.db.update(req.url, JSON.stringify(mark), function filter(prev) {
    // Allow updates only if password matches to previous
    prev = JSON.parse(prev.toString());
    return prev.password === mark.password;
  }, function(err) {
    res.setHeader('content-type', 'application/json');
    if (err) {
      res.writeHead(400);
      res.end('{"err":"incorrect data"}');
    } else {
      res.writeHead(200);
      res.end('{"status":"ok"}');
    }
  });
};

exports.create = function(db) {
  var instance = new Marks(db);

  return function(req, res, next) {
    if (req.method === 'GET') {
      instance.get(req, res, function() {
        res.redirect('/');
      });
    } else if (req.method === 'POST') {
      instance.set(req, res);
    } else {
      next();
    }
  };
};
