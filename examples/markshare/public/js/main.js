!function() {
  var form = $('#doc-form'),
      status = $('#status'),
      fields = {
        url: $('input#url', form),
        password: $('input#password', form),
        title: $('input#title', form),
        body: $('textarea#body', form),
        button: $('input[type=submit]', form)
      };

  function fieldsSwitch(disabled) {
    fields.url.attr('disabled', disabled);
    fields.password.attr('disabled', disabled);
    fields.title.attr('disabled', disabled);
    fields.body.attr('disabled', disabled);
    fields.button.attr('disabled', disabled);
  }

  function showStatus(type, message, callback) {
    if (status.is(':visible')) {
      status.stop().slideUp(notVisible);
    } else {
      notVisible();
    }
    function notVisible() {
      status.removeClass('success').removeClass('error');
      status.addClass(type);

      status.text(message);
      status.slideDown().delay(1100).slideUp(callback);
    }
  }

  form.submit(function() {
    var url = fields.url.val().replace(/^\/+/, '');

    fieldsSwitch(true);

    $.ajax({
      url: '/' + url,
      type: 'POST',
      contentType: 'application/json',
      data: JSON.stringify({
        password: fields.password.val(),
        title: fields.title.val(),
        body: fields.body.val()
      }),
      success: response,
      error: function(xhr) {
        try {
          var data = JSON.parse(xhr.responseText);
          response(data);
        } catch (e) {
        }
        fieldsSwitch(false);
      }
    });

    function response(data) {
      var texts = {
        success: 'Mark was successfully created. ' +
                 'You\'ll be automatically redirect to it in a second',
        error: 'Can\'t create mark! ' +
               'Probably mark with same url already exists! ' +
               'Or password doesn\'t match!'
      };
      if (data.status === 'ok') {
        showStatus('success', texts.success, function() {
          location.href = '/' + url;
        });
      } else {
        showStatus('error', texts.error, function() {
        });
      }
    }
  });
}();
