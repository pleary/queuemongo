var expect = require('chai').expect,
    QueueMongo = require('../lib/QueueMongo'),
    queue = null,
    hook = null;

describe('QueueMongo', function() {
  before(function(done) {
    queue = new QueueMongo('127.0.0.1', 'test', function(err) {
      queue.db.dropCollection('test', done);
    });
  });

  describe('#initialize', function() {
    it('creates an instance of QueueMongo', function() {
      expect(queue).to.be.an.instanceof(QueueMongo);
    });

    it('returns an error on connection failure', function(done) {
      new QueueMongo('wrongDbHost', 'test', function(err) {
        expect(err).to.be.an.instanceof(Error);
        expect(err.message).to.eq('failed to connect to [wrongDbHost:27017]');
        done();
      });
    });
  });

  describe('#pushItem', function() {
    it('pushes items to the end of the queue', function(done) {
      queue.pushItem({ name: 'Tom' }, function() {
        queue.pushItem({ name: 'Jerry' }, function() {
          queue.db.collection('test').find({ name: { $exists: true } }).toArray(function(err, docs) {
            expect(docs.length).to.be.at.least(2);
            expect(docs[0].name).to.eq('Tom');
            expect(docs[0].status).to.eq('pending');
            expect(docs[1].name).to.eq('Jerry');
            expect(docs[1].status).to.eq('pending');
            done();
          });
        });
      });
    });

    it('allows a type to be set', function(done) {
      queue.pushItem({ }, function(err, doc) {
        expect(doc.type).to.eq('default');
        done();
      });
    });

    it('sets a default type', function(done) {
      queue.pushItem({ type: 'custom' }, function(err, doc) {
        expect(doc.type).to.eq('custom');
        done();
      });
    });

    it('throws an error when bad items are sent', function(done) {
      queue.pushItem({ $fail: true }, function(err) {
        expect(err).to.be.an.instanceof(Error);
        done();
      });
    });
  });

  describe('#popItem', function() {
    it('throws an error when a bad sort field is sent', function(done) {
      queue.popItem([[[ ], 1]], function(){}, function(err, doc) {
        expect(err).to.be.an.instanceof(Error);
        expect(doc).to.eq(undefined);
        done();
      });
    });
  });

  describe('#finishItem', function() {
    it('pushes items to the end of the queue', function(done) {
      queue.pushItem({ age: 1 }, function(err, doc) {
        queue.finishItem(doc);
        queue.pushItem({ age: 2 }, function(err, doc) {
          queue.finishItem(doc, 'failed');
          queue.pushItem({ age: 3 }, function(err, doc) {
            queue.finishItem(doc, 'anything');
            queue.db.collection('test').find({ age: { $exists: true } }).toArray(function(err, docs) {
              expect(docs.length).to.be.at.least(3);
              expect(docs[0].status).to.eq('done');
              expect(docs[1].status).to.eq('failed');
              expect(docs[2].status).to.eq('anything');
              done();
            });
          });
        });
      });
    });
  });

  describe('#actOnItem', function() {
    it('runs the default handler', function(done) {
      var item = queue.pushItem({ }, function(err, item) {
        queue.actOnItem(item, defaultItemHandler, function(err, doc) {
          expect(doc.status).to.eq('done');
          done();
        });
      });
    });

    it('adds a newline to stdout if there was a waiting indicator', function() {
      queue.pushItem({ }, function(err, item) {
        hook = captureStream(process.stdout);
        for(var i=0 ; i < 19 ; i++) queue.wait();
        queue.actOnItem(item, function() { });
        expect(hook.captured()).to.eq('Waiting.............\n');
        hook.unhook();
      });
    });
  });

  describe('#updateItemStatus', function() {
    it('changes status', function(done) {
      queue.pushItem({ color: 'Blue' }, function(err, item) {
        queue.updateItemStatus(item, 'new status', function(err, doc) {
          expect(doc.status).to.eq('new status');
          done();
        });
      });
    });

    it('returns an error when bad items are sent', function(done) {
      queue.pushItem({ }, function(err, item) {
        queue.updateItemStatus({ $fail: true }, 'new status', function(err, doc) {
          expect(err).to.be.an.instanceof(Error);
          expect(err.message).to.eq('Unknown modifier: $fail');
          done();
        });
      });
    });

    it('does nothing with a bad document', function(done) {
      queue.updateItemStatus(null, 'new status', function(err, doc) {
        expect(err).to.eq(null);
        expect(doc).to.eq(null);
        done();
      });
    });
  });

  describe('#iterate', function() {
    it('pops items off the queue as it iterates', function(done) {
      queue.pushItem({ age: 1 }, function(err, doc) {
        queue.iterate(defaultItemHandler, function() {
          // after iterating the queue will be empty
          queue.popItem(defaultItemHandler, function(err, doc) {
            expect(doc).to.eq(null);
            done();
          });
        });
      });
    });

    it('will iterate forever if not given a completion callback', function(done) {
      hook = captureStream(process.stdout);
      queue.iterate();
      setTimeout(function() {
        hook.unhook();
        setTimeout(function() {
          expect(hook.captured()).to.match(/Waiting/);
          done();
        }, 10);
      }, 40);
    });
  });

  describe('#wait', function() {
    beforeEach(function() {
      // this hook will temporarily redirect stdout to avoid
      // some extra newlines that might get output from pop
      hook = captureStream(process.stdout);
      // pop resets the waiting indicator
      queue.actOnItem(null, function() { });
      hook.unhook();
    });

    it('displays a waiting message after 5 failed pops', function() {
      hook = captureStream(process.stdout);
      for(var i=0 ; i < 5 ; i++) queue.wait();
      expect(hook.captured()).to.eq('');
      queue.wait();
      expect(hook.captured()).to.eq('Waiting');
      hook.unhook();
    });

    it('displays a waiting indicator as the wait grows longer', function() {
      hook = captureStream(process.stdout);
      for(var i=0 ; i < 20 ; i++) queue.wait();
      expect(hook.captured()).to.eq('Waiting..............');
      hook.unhook();
    });
  });
});

var defaultItemHandler = function(doc, finish, callback) {
  setTimeout(function() {
    finish(doc, 'done', callback);
  }, 0);
};

var captureStream = function(stream) {
  var oldWrite = stream.write;
  var buf = '';
  stream.write = function(chunk, encoding, callback){
    buf += chunk.toString();
  }
  return {
    unhook: function unhook() {
     stream.write = oldWrite;
    },
    captured: function() {
      return buf;
    }
  }
};
