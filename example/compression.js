'use strict';

//var snappy = require('snappy');
//var sss = require('fs').readFileSync("snappysss");
//console.log("FILE ", sss);
//return;
//for(var iD = 0; iD < 100; iD++)
//{
//  (function(i) {
//    var str = sss.toString().substr(i);
//    //console.log(i + "## ",str);
//    snappy.uncompress(new Buffer(str), { asBuffer: false }, function (err, original) {
//      if (err)
//        console.log("\n", i, err, str);
//      else
//        console.log('OKKKK');
//    });
//  })(iD);
//}
//
//return;
//
//console.log("�", "and", "\x82");
//if ("�" == "\x82")
//{
//  console.log('youou');
//}
//// Special thanks to Colin Blower
//if ( header ==  "\x82SNAPPY\x00" )
//{
//  console.log('yeahhh');
//}
//
//// Found a xerial header.... nonstandard snappy compression header, remove the header
////  if ( $x_compatversion == 1 && $x_version == 1 ) {
////    $Message->{Value} = substr( $Message->{Value}, 20 );    # 20 = q{a[8]L>L>L>}
////  } else {
////  #warn("V $x_version and comp $x_compatversion");
////    _error( $ERROR_COMPRESSION, "Snappy compression with incompatible xerial header version found (x_version = $x_version, x_compatversion = $x_compatversion)" );
////  }
////}
////
////
//snappy.compress('beep boop', function (err, compressed) {
//  console.log('compressed is a Buffer', compressed.toString())
//  // return it as a string
//  snappy.uncompress(compressed, { asBuffer: false }, function (err, original) {
//    console.log('the original String', original)
//  })
//})
////
////
////var buf = new Buffer(s);
////console.log('loooool', buf);
////snappy.uncompress(buf, { asBuffer: false }, function (err, original) {
////  console.log('VALUEUUUEU original String', original, err)
////});

var kafka = require('../kafka');
var HighLevelConsumer = kafka.HighLevelConsumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var argv = require('optimist').argv;
var topic = argv.topic;
if (!topic)
  throw new Error('provide --topic mytopic');

var client = new Client('kafka:2181');
var topics = [ { topic: topic }];
var options = {
  groupId: 'kafka-node-group-consumer',
  autoCommitIntervalMs: 5000,
  fromOffset: false,
  /*autoCommit: false, fromBeginning: false,*/
  fetchMaxWaitMs: 1000
  /*, fetchMaxBytes: 1024*1024 */
};
var consumer = new HighLevelConsumer(client, topics, options);
var offset = new Offset(client);

consumer.on('message', function (message) {
  console.log(message);
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

