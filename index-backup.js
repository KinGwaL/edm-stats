require('dotenv').config();
const Kafka      = require('node-rdkafka');
const express    = require('express');
const URL        = require('url');
const fs         = require('fs');
const { Pool, Client } = require('pg');
const format = require('pg-format');
const parseDbUrl       = require('parse-database-url');
const PORT       = process.env.PORT || 5002;
const nodeEnv    = process.env.NODE_ENV || 'development';
const sslFlag = (nodeEnv == "development") ? false : true;









//Postgres DB Config & Connection
const dbConfig = parseDbUrl(process.env["DATABASE_URL"]);
const pool = new Pool({
  user: dbConfig.user,
  host: dbConfig.host,
  database: dbConfig.database,
  password: dbConfig.password,
  port: dbConfig.port,
  ssl: sslFlag
})







//
// Kafka Connection 
//
var consumer = new Kafka.KafkaConsumer({
  'client.id': `edm/${process.env.DYNO || 'localhost'}`,
  'group.id': `${process.env.KAFKA_PREFIX}${process.env.KAFKA_CONSUMER_GROUP}`,
  'metadata.broker.list': brokerHostnames.toString(),
  'security.protocol': 'SSL',
  'ssl.ca.location': "tmp/env/KAFKA_TRUSTED_CERT",
  'ssl.certificate.location': "tmp/env/KAFKA_CLIENT_CERT",
  'ssl.key.location': "tmp/env/KAFKA_CLIENT_CERT_KEY",
  'enable.auto.commit': false,
  'offset_commit_cb': function(err, topicPartitions) {}
}, {});
consumer.connect({}, (err, data) => {});







//
// Kafka Receive Topic 
//
consumer
  .on('ready', (id, metadata) => {
    console.log(kafkaTopics);
    consumer.subscribe(kafkaTopics); 
    consumer.consume();
    consumer.on('error', err => {});
    clearTimeout(connectTimoutId);
  })
  .on('data', function(data) {
    const message = data.value.toString()
    const json = JSON.parse(message);
    switch (json.topic) {
      case KAFKA_ANALYTIC_TOPIC:
        interactiveStudio.GeneralTrigger(json);
        break;
		  case KAFKA_TRANSACTION_TOPIC:
        interactiveStudioTrigger(json);
        break;
    }
  });







//
// Kafka IS Trigger
//
function interactiveStudioTrigger(data) {
  const currencyData = data["Request"]["ForeignCurry"];
  const priceData = data["Response"]["CalculatedHKDAmount"];
  const userId = data["Request"]["RmNo"];

  const json = {
    "action": "CITIC - Transaction Data To IS",
    "user": {
      "id": userId,
    },
    "itemAction": "Purchase",
    "order": {
      "Product": {
        "orderId": uuid.v1(),
        "currency": currencyData,
        "lineItems":[{
          "_id":currencyData,
          "price": priceData,
          "quantity": 1
        }]
      }
    }
  };

  fetch(INTERACTION_STUDIO_ROOT, {
    method: "POST",
    body: JSON.stringify(json),
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    }
  }).then(function(response) {
    fireGeneralTrigger(json);
  }, function(error) {
    dataStore.store(json, error);
    setInterval(2000, dataStore.missingRecord());
  });
}







//
// Server Config
//
const app = express();
app.use(function(req,res,next){
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader("Access-Control-Allow-Methods", "GET");
  res.setHeader("Access-Control-Allow-Headers", "Access-Control-Allow-Headers, Origin, Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers");
  next();
})

app.use(function (err, req, res, next) {
  console.error(err.stack)
  res.status(500).send('Error calling ')
})

app.listen(PORT, function () {
  console.log(`Listening on port ${PORT}`);
});