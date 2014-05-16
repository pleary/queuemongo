var mongodb = require('mongodb');
var mongoClient = mongodb.MongoClient;
var _ = require('underscore');
var currentWaitTime = 0;
var MAXIMUM_WAIT_TYPE = 2000;
var number_of_consecutive_waits = 0;

var QueueMongo = function(mongodb_uri, queueName, callback) {
  this.queueName = queueName;
  var q = this;
  // QueueMongo uses its own database named QueueMongo
  mongoClient.connect(mongodb_uri + '/QueueMongo', function(err, d) {
    if(err) return callback(err);
    q.db = d;
    // making sure the queue collection has the proper indices
    q.db.createCollection(q.queueName, function(err, collection) {
      q.db.collection(q.queueName).ensureIndex({ status: 1 }, function() {
        q.db.collection(q.queueName).ensureIndex({ timestamp: 1 }, function() {
          callback();
        });
      });
    });
  });
}

QueueMongo.prototype.db = null;
QueueMongo.prototype.queueName = null;

QueueMongo.prototype.iterate = function(queueItemCallback, completionCallback) {
  var q = this;
  this.popItem(null, queueItemCallback, function(err, doc) {
    if(!err && !doc) {
      if(_.isFunction(completionCallback)) return completionCallback();
      q.wait();
    } else if(err === 'at capacity') q.wait();
    setTimeout(function() {
      q.iterate(queueItemCallback, completionCallback);
    }, currentWaitTime);
  });
};

QueueMongo.prototype.popItem = function(sortBy, queueItemCallback, callback) {
  if(callback === undefined && _.isFunction(sortBy)) {
    callback = queueItemCallback;
    queueItemCallback = sortBy;
    sortBy = null;
  }
  var q = this;
  this.collection().count({ status: 'in-progress' }, function(err, count) {
    if(err || count > 0) {
      // currently allowing only one running task
      return callback('at capacity');
    }
    q.db.collection(q.queueName).findAndModify(
      { status: { $nin: [ 'in-progress', 'done', 'failed', 'unrecognized type' ] } },
      sortBy || { timestamp: 1 },
      { $set: { status: 'in-progress' } },
      function(err, doc) {
        if(err) return callback(err);
        if(doc === null) {
          // if doc is null then there were no results. Queue is empty
          return callback(null, null);
        }
        callback(doc);
        q.actOnItem(doc, queueItemCallback);
      }
    );
  });
};

QueueMongo.prototype.wait = function() {
  if(number_of_consecutive_waits === 5) process.stdout.write('Waiting');
  else if(number_of_consecutive_waits > 5) process.stdout.write('.');
  number_of_consecutive_waits++;
  if(currentWaitTime === 0) currentWaitTime = 1;
  else currentWaitTime *= 1.5;
  if(currentWaitTime > MAXIMUM_WAIT_TYPE) currentWaitTime = MAXIMUM_WAIT_TYPE;
};

QueueMongo.prototype.pushItem = function(doc, callback) {
  var docToInsert = _.defaults(doc, { status: 'pending', timestamp: new Date() });
  this.collection().insert(docToInsert, function(err, docs) {
    callback(err, (docs ? docs[0] : null));
  });
};

QueueMongo.prototype.actOnItem = function(doc, queueItemCallback, callback) {
  if(number_of_consecutive_waits >= 5) process.stdout.write('\n');
  number_of_consecutive_waits = 0;
  currentWaitTime = 0;
  queueItemCallback(doc, this.finishItem.bind(this), callback);
};

QueueMongo.prototype.finishItem = function(doc, status, callback) {
  if(status === undefined) status = 'done'
  this.updateItemStatus(doc, status, callback);
};

QueueMongo.prototype.updateItemStatus = function(doc, status, callback) {
  if(!doc) return callback(null, null);
  var docToInsert = _.extend(doc, { status: status, timestamp: new Date() });
  this.collection().update({ _id: docToInsert['_id'] }, docToInsert, function(err, doc) {
    if(err) return callback(err);
    if(_.isFunction(callback)) callback(err, docToInsert);
  });
};

QueueMongo.prototype.collection = function(callback) {
  return this.db.collection(this.queueName);
};

QueueMongo.prototype.empty = function(callback) {
  this.collection().remove({}, function() {
    callback();
  });
};

module.exports = QueueMongo;
