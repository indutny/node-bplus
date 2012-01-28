
/**
 * Module dependencies.
 */

var express = require('express'),
    routes = require('./routes'),
    bplus = require('../../'),
    marks = require('./marks/api');

var app = module.exports = express.createServer();

// Configuration

app.db = bplus.create().open(__dirname + '/' + app.settings.env + '.bp');

app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'jade');
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(express.staticCache());
  app.use(express.static(__dirname + '/public'));
  app.use(app.router);

  /* all non-matched requests will go to app-specific router */
  app.use(marks.create(app.db));
});

app.configure('development', function(){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
  app.use(express.errorHandler());
});

// Routes

app.get('/', routes.index);

app.listen(3000);
console.log(
    "Express server listening on port %d in %s mode",
    app.address().port,
    app.settings.env
);

process.on('exit', function() {
  app.db.close();
});
