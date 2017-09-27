var natsNodeStreaming = require('node-nats-streaming');
var riemann = require('riemann');
var colors = require('colors');


function createRiemannConnection() {
  return new Promise((resolve, reject) => {
    var riemannClient = riemann.createClient({
      host: "localhost",
      port: 5555
    });

    riemannClient.on('connect', () => {
      console.log('Riemann - connected');
      resolve(riemannClient);
    });

  });
}


function createNatsConnection() {
  return new Promise((resolve, reject) => {
    var srv = 'nats://localhost:4222'
    var server = natsNodeStreaming.connect('test-cluster', 'thingenix-riemann-pusher', srv);
    server.on('connect', function () {
      console.log('NATS - connected');
      resolve(server);
    });
  });
}


// main
Promise.all([createRiemannConnection(), createNatsConnection()])
.then(([riemannConnection, natsConnection]) => {
  console.log('Connected to Riemann & NATS');

  var opts = natsConnection.subscriptionOptions().setDeliverAllAvailable();
  var natsSubscription = natsConnection.subscribe('thingenix', opts);

  var i = 0;
  natsSubscription.on('message', (msg) => {
    // message is Buffer

    if (i++ % 50000 == 0) {
      console.log("Got from Nats".underline.blue, i, "messages".underline.blue);
      console.log(msg.getData());
    }

    var message = JSON.parse(msg.getData());
    for (a in message["items"]) {
      if ( message["timestamp"] === undefined ) {
         continue;
      }
      var d = new Date(message["timestamp"]);
      riemannConnection.send(riemannConnection.Event({
          host: message["_id"],
          service: a,
          time: d.getTime() / 1000,
          metric: message["items"][a],
          attributes: [{key:"longitude", value: message["loc"][0]}, {key:"latitude", value: message["loc"][1]},{key:"context", value: message["context"]}], //custom fields
          ttl: 10
        }), riemannConnection.tcp);
    }
  });
})
.catch((err) => {
  console.log(err);
});
