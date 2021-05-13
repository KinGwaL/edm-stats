const nodeEnv = process.env.NODE_ENV || 'development';

let devClickTopicName;
let devPageloadTopicName;
let generalTopicName;

if(nodeEnv == "development") {
  devClickTopicName = "edm-ui-click-local";
  devPageloadTopicName = "edm-ui-pageload-local";
  generalTopicName = "citic-poc-general-response";
} else {
  devClickTopicName = "citic-poc-transaction-request";
  devPageloadTopicName = "citic-poc-transaction-response";
  generalTopicName = "citic-poc-general-response";
}

const CLICK_KAFKA_TOPIC     = process.env.CLICK_KAFKA_TOPIC || devClickTopicName;
const PAGE_LOAD_KAFKA_TOPIC = process.env.PAGE_LOAD_KAFKA_TOPIC || devPageloadTopicName;
const GENERAL_TOPIC = generalTopicName;

module.exports = {
  CLICK_KAFKA_TOPIC,
  PAGE_LOAD_KAFKA_TOPIC,
  GENERAL_TOPIC
}