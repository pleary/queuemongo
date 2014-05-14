QueueMongo
=========

[![Coverage Status](https://coveralls.io/repos/pleary/queuemongo/badge.png)](https://coveralls.io/r/pleary/queuemongo)

QueueMongo is a node.js client for a queue service built over MongoDB.

## Usage

Creating an instance of a queue
```javascript
var QueueMongo = require('queuemongo');

var q = new QueueMongo('127.0.0.1:27017', 'myQueue', function(err) { });
```

Adding items to a queue
```javascript
q.pushItem({ attr: 'val' }, function(err, doc) { });
```

Iterating through a queue with a specified callback
```javascript
q.iterate(
  function(doc, finish) {
    console.log(doc);
    finish(doc);
  },
  function() {
    q.db.close();
  }
);
```

## Tests

You can run the tests with `npm test`, or run with coverage reporting with `npm test --coverage`.

## Release History

* 0.1.0 Initial release
