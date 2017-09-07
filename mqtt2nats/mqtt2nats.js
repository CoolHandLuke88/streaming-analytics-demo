var mqtt = require('mqtt');
var natsNodeStreaming = require('node-nats-streaming');
var colors = require('colors');


function createMqttConnection() {
  return new Promise((resolve, reject) => {
    var mqttConnection  = mqtt.connect('mqtts://beta.test.io', {
      username: 'demo@test.com',
      password: 'demo',
    });

    mqttConnection.on('connect', () => {
      console.log('MQTT - connected');
      resolve(mqttConnection);
    });

  });
}


function createNatsConnection() {
  return new Promise((resolve, reject) => {
    var server = natsNodeStreaming.connect('test-cluster', 'thingenix-mqtt-proxy');
    server.on('connect', function () {
      console.log('NATS - connected');
      resolve(server);
    });
  });
}




// main
Promise.all([createMqttConnection(), createNatsConnection()])
.then(([mqttConnection, natsConnection]) => {
  console.log('Connected to MQTT & NATS');

  mqttConnection.on('message', (topic, message) => {
    // message is Buffer
    console.log(topic.toString().underline.blue, message.toString());

    natsConnection.publish('thingenix', message.toString(), (err, guid) => {
      if(err) {
        console.log('publish failed: '.red, err);
      } else {
        console.log('published message with guid: '.green + guid);
      }
    });
  });

  mqttConnection.subscribe('592c8451da64856b397fcdfe/#');       // Industrial   -  Test context for SensorHUBs with vibration and temperature sensors
  mqttConnection.subscribe('594e7bfb463aee1301da2d6b/#');       // Oil & Gas    -  Oil & Gas
  mqttConnection.subscribe('594e7c0a463aee1301da2d6d/#');       // Agriculture  -  Agriculture
  mqttConnection.subscribe('594e7c1b463aee1301da2d6f/#');       // Solar        - SensorHUB Solar Panel Testing
})
.catch((err) => {
  console.log(err);
});
