
/*
 * GET home page.
 */

exports.index = function(req, res){
  res.render('edit', {
    title: '',
    mark: {
      url: '',
      title: '',
      body: ''
    }
  });
};

/*
 * REST interface
 */
exports.getMark = function getMark(req, res) {
  res.end('ok');
};

exports.createMark = function getMark(req, res) {
  res.end('ok');
};
