'use strict';

var kafka = require('../kafka');
var HighLevelConsumer = kafka.HighLevelConsumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic;
var fs = require('fs');
if (!topic)
  throw new Error('provide --topic mytopic');

var client = new Client('kafka:2181');
var topics = [ { topic: topic }];
var options = {
  groupId: 'kafka-node-group-consumer',
  autoCommitIntervalMs: 5000,
  fromOffset: false,
  fetchMaxWaitMs: 1000
  /*, fetchMaxBytes: 1024*1024 */
};
var consumer = new HighLevelConsumer(client, topics, options);
var offset = new Offset(client);

var i = 0;
consumer.on('message', function (message) {
  i++;
  console.log(i);


  fs.appendFileSync('kafka.txt', JSON.stringify(message) + "\n");

  if (i == 50000)
    process.exit();
});
consumer.on('error', function (err) {
  console.log('error', err);
});
consumer.on('offsetOutOfRange', function (topic) {
  topic.maxNum = 2;
  offset.fetch([topic], function (err, offsets) {
    var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
    consumer.setOffset(topic.topic, topic.partition, min);
  });
});

