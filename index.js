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

const currentPath  = process.cwd();
const uuid = require('uuid');
const fetch = require("node-fetch");

const { CLICK_KAFKA_TOPIC, PAGE_LOAD_KAFKA_TOPIC,GENERAL_TOPIC } = require('./kafka-topics.js')
//const { API_ROOT } = require('./api-config');
const API_ROOT = process.env.REACT_APP_EDM_RELAY_BACKEND_HOST;

if (!process.env.KAFKA_PREFIX)          throw new Error('KAFKA_PREFIX is not set.')
if (!process.env.KAFKA_URL)             throw new Error('KAFKA_URL is not set.')
if (!process.env.KAFKA_CONSUMER_GROUP)  throw new Error('KAFKA_CONSUMER_GROUP is not set.')
if (!process.env.KAFKA_TOPIC)           throw new Error('KAFKA_TOPIC is not set.')
if (!process.env.KAFKA_TRUSTED_CERT)    throw new Error('KAFKA_TRUSTED_CERT is not set.')
if (!process.env.KAFKA_CLIENT_CERT)     throw new Error('KAFKA_CLIENT_CERT is not set.')
if (!process.env.KAFKA_CLIENT_CERT_KEY) throw new Error('KAFKA_CLIENT_CERT_KEY is not set.')
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not set.')

if (!fs.existsSync('tmp/env/KAFKA_TRUSTED_CERT')) {
	throw new Error('KAFKA_TRUSTED_CERT has not been written to file. Try executing the .profile script.');
}
if (!fs.existsSync('tmp/env/KAFKA_CLIENT_CERT')) {
	throw new Error('KAFKA_CLIENT_CERT has not been written to file. Try executing the .profile script.');
}
if (!fs.existsSync('tmp/env/KAFKA_CLIENT_CERT_KEY')) {
	throw new Error('KAFKA_CLIENT_CERT_KEY has not been written to file. Try executing the .profile script.');
}

//Postgres Config
const dbConfig = parseDbUrl(process.env["DATABASE_URL"]);

// Connect to postgres
const pool = new Pool({
  user: dbConfig.user,
  host: dbConfig.host,
  database: dbConfig.database,
  password: dbConfig.password,
  port: dbConfig.port,
  ssl: sslFlag
})

// Kafka Config
// For multi-tenant kafka on heroku, we must prefix each topic
const kafkaTopicsString=process.env.KAFKA_TOPIC;
let kafkaTopics = kafkaTopicsString.split(",");
kafkaTopics = kafkaTopics.map((topic)=>{
  return `${process.env.KAFKA_PREFIX}${topic}`
});

// split up the comma separated list of broker urls into an array
const kafkaBrokerUrls = process.env.KAFKA_URL;
let brokerHostnames = kafkaBrokerUrls.split(",").map((u)=>{
  return URL.parse(u).host;
});

// throw an error if we don't connect to the broker in 5 seconds
// causes the heroku app to crash and retry
const connectTimeout = 5000;
const connectTimoutId = setTimeout(() => {
      const message = `Failed to connect Kafka consumer (${connectTimeout}-ms timeout)`;
      const e = new Error(message);
      throw e;
    }, connectTimeout)

//
// Kafka Consumer 
//
var consumer = new Kafka.KafkaConsumer({
  // 'debug': 'all',
  'client.id':                `edm/${process.env.DYNO || 'localhost'}`,
  'group.id': `${process.env.KAFKA_PREFIX}${process.env.KAFKA_CONSUMER_GROUP}`,
  'metadata.broker.list': brokerHostnames.toString(),
  'security.protocol': 'SSL',
  'ssl.ca.location':          "tmp/env/KAFKA_TRUSTED_CERT",
  'ssl.certificate.location': "tmp/env/KAFKA_CLIENT_CERT",
  'ssl.key.location':         "tmp/env/KAFKA_CLIENT_CERT_KEY",
  'enable.auto.commit': false,
  'offset_commit_cb': function(err, topicPartitions) {
    if (err) {
      // There was an error committing
      console.error("There was an error committing");
      console.error(err);
    } else {
      // Commit went through. Let's log the topic partitions
      console.log("New offset successfully committed.")
    }
  }
}, {});

consumer.connect({}, (err, data) => {
  if(err) {
    console.error(`Consumer connection failed: ${err}`);
  }else {
    console.log(`Connection to kafka broker successful: ${JSON.stringify(data)}`)
  }
});


let productClicks = []
let pageLoads = 0;

//save clicks and page loads every 60 seconds, or every 5 seconds locally
setInterval(saveStatsToPostgres, sslFlag ? 5000 : 5000);

function saveStatsToPostgres() {
  let newClicks = false;
  let newLoads = false;

  var date = new Date();

  // let clickValues = Object.keys(productClicks).map(key => {
  //   return [key,productClicks[key]]
  // });

  let clickValues = [];
  
  productClicks.forEach(key => {
    clickValues.push([key["txn_id"],key["foriegn_curry"],key["c_date_time"],key["req_type"],key["rm_no"]]);
  });

  let clickEventQuery = format('INSERT INTO transaction_request(txn_id,foriegn_curry,c_date_time,req_type,rm_no) VALUES %L', clickValues);
  console.log(clickEventQuery);
  if (clickValues.length > 0) newClicks = true;

  const pageLoadValues = [pageLoads, date.getTime()];
  let loadEventQuery = 'INSERT INTO page_load(loads,created_date) VALUES($1, to_timestamp($2 / 1000.0))';
  if (pageLoads > 0) newLoads = true;
  
  if (!newClicks && !newLoads) {
    console.log('no new events to record!')
  } else {
    (async () => {
      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        let rows;
        let rows2;
        if (newClicks){
          rows = await client.query(clickEventQuery)
        } 
        if (newLoads) {
          rows2 = await client.query(loadEventQuery, pageLoadValues)
        } 
        await client.query('COMMIT')
      } catch (e) {
        await client.query('ROLLBACK')
        throw e
      } finally {
        client.release()
        console.log('successfully saved data to postgres. committing new offset.')
        consumer.commit();
        productClicks = [];
        pageLoads = 0;
      }
    })().catch(e => console.error(e.stack))
  }
}

function transactionResponse(json) {
  console.log(json);
  const responseData = json["properties"];
  const txnId = responseData["TxnId"];
  if (!txnId) {
    return;
  }
  // const transactionSql = `SELECT * FROM transaction_request WHERE txn_id = '${txnId}'`;
  // pool.query(transactionSql)
  //     .then(pgResponse => {
  //      console.log("BBB");
  //      if(pgResponse.rows.length <= 0) {
  //        return;
  //      }
       
  //      const rowData = pgResponse.rows[0];
  //      const fullDateString = rowData["c_date_time"];
  //      const yearString = fullDateString.substring(0, 4);
  //      const monthString = fullDateString.substring(4, 6);
  //      const dateString = fullDateString.substring(7, 9);

  //      const isData = {
  //       "Transaction_Date": `${yearString}-${monthString}-${dateString}`,
  //       "Currency": rowData["foriegn_curry"],
  //       "Transaction_amount_HKD": responseData["calculated_hkd_amount"],
  //       "CustomerID": rowData["rm_no"]
  //     };

  //     fireGeneralTrigger(isData);
  //   })
  //   .catch(error =>{
  //     console.log("transactionResponse-error");
  //     console.log(error);
  //   });


  //fireGeneralTrigger(responseData);
  interactiveStudioTrigger(responseData);
}


function interactiveStudioTrigger(data) {
  //console.log(data);
  const currencyData = data["Request"]["ForeignCurry"];
  const priceData = data["Response"]["CalculatedHKDAmount"];
  const userId = data["Request"]["RmNo"];
  const fullDateString = data["Request"]["CDateTime"];
  const yearString = fullDateString.substring(0, 4);
  const monthString = fullDateString.substring(4, 6);
  const dateString = fullDateString.substring(7, 9);

  const json = {
    "action": "CITIC - Transaction Data To IS",
    "user": {
      "id": userId,
      "attributes": {
        "customerid": userId,
        "transactionDataFields":`IS___${currencyData}___${priceData}___${yearString}-${monthString}-${dateString}____citic@tokenization.com`,
        "transactionFlags": true
      }
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
  // send message
  fetch("https://partnerdeloittechina.australia-3.evergage.com/api2/event/macstudy", {
    method: "POST",
    body: JSON.stringify(json),
    headers: {
      Accept: 'application/json',
      // origin: window.location.hostname,
      'Content-Type': 'application/json',
    }
  }).then(function(response) {
    console.log(response);
    console.log(json);
    //const res = response.json();
    //fireGeneralTrigger(json);
    
  //  next();
    //return res;
  }, function(error) {
    console.log("fireGeneralTrigger-error");
    console.error(error.message);
  });
}


function fireGeneralTrigger(data) {
  console.log(data);
  const json = {
    "topic": GENERAL_TOPIC,
    "uuid": uuid.v1(),
    "event_timestamp": Date.now(),
    "properties": data
  };
  // send message
  fetch(`${API_ROOT}/fireTrigger`, {
    method: "POST",
    body: JSON.stringify(json),
    headers: {
      Accept: 'application/json',
      // origin: window.location.hostname,
      'Content-Type': 'application/json',
    }
  }).then(function(response) {
    const res = response.json();
    console.log(res);
  //  next();
    //return res;
  }, function(error) {
    console.log("fireGeneralTrigger-error");
    console.error(error.message);
  });
}

consumer
  .on('ready', (id, metadata) => {
    console.log(kafkaTopics);
    consumer.subscribe(kafkaTopics); 
    consumer.consume();
    consumer.on('error', err => {
      console.log(`!      Error in Kafka consumer: ${err.stack}`);
    });
    console.log('Kafka consumer ready.' + JSON.stringify(metadata));
    clearTimeout(connectTimoutId);
  })
  .on('data', function(data) {
    const message = data.value.toString()
    const json = JSON.parse(message);
    console.log(data);

    switch (json.topic) {
    	case CLICK_KAFKA_TOPIC:
        if(json.properties.hasOwnProperty('txn_id')) productClicks.push(json.properties);
        // if (json.properties.button_id in productClicks) productClicks[json.properties.button_id]++;
        // else productClicks[json.properties.button_id] = 1;
			  break;
		  case PAGE_LOAD_KAFKA_TOPIC:
        transactionResponse(json);
        //pageLoads+=1;
        break;
      case GENERAL_TOPIC:
        break;
    }
  })
  .on('event.log', function(log) {
    console.log(log);
  })
  .on('event.error', function(err) {
    console.error('Error from consumer');
    console.error(err);
  });


//
// Server
//
const app = express();


app.use(function(req,res,next){
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader("Access-Control-Allow-Methods", "GET");
  res.setHeader("Access-Control-Allow-Headers", "Access-Control-Allow-Headers, Origin, Accept, X-Requested-With, Content-Type, Access-Control-Request-Method, Access-Control-Request-Headers");
  next();
})

// returns the number of clicks per button in the db
//'select row_to_json(t) from ( select button_id, count(button_id) from button_click group by button_id) t'
app.get('/api/clickCount', (req, res, next) => {
  const clickEventSql = 'SELECT foriegn_curry, count(foriegn_curry) FROM transaction_request GROUP BY foriegn_curry';
  pool.query(clickEventSql)
      .then(pgResponse => {
      // console.log(pgResponse);
      res.setHeader('Content-Type', 'application/json');
      res.send(JSON.stringify(pgResponse.rows));
      next();
    })
    .catch(error =>{
      next(error);
    });
})

app.get('/api/clickHistory', (req, res, next) => {
  const clickEventSql = 'SELECT date_trunc(\'day\', transaction_request.created_date) AS "Day" , count(foriegn_curry) AS "transactions" FROM transaction_request GROUP BY 1 ORDER BY 1';
  pool.query(clickEventSql)
      .then(pgResponse => {
      // console.log(pgResponse);
      res.setHeader('Content-Type', 'application/json');
      res.send(JSON.stringify(pgResponse.rows));
      next();
    })
    .catch(error =>{
      next(error);
    });
})

app.use(function (err, req, res, next) {
  console.error(err.stack)
  res.status(500).send('Error calling ')
})

app.listen(PORT, function () {
  console.log(`Listening on port ${PORT}`);
});