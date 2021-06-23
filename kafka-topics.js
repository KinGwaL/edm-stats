const nodeEnv = process.env.NODE_ENV || 'development';

// Kafka Topic Name
let devClickTopicName = "citic-poc-transaction-request";
let devPageloadTopicName = "citic-poc-transaction-response";
let generalTopicName = "citic-poc-general-response";
let directTopicName = "citic-poc-mobile-action";

// Global Topic Env
const CLICK_KAFKA_TOPIC = process.env.CLICK_KAFKA_TOPIC || devClickTopicName;
const TRANSACTION_TOPIC = process.env.PAGE_LOAD_KAFKA_TOPIC || devPageloadTopicName;
const GENERAL_TOPIC = generalTopicName;
const DIRECT_ACTION_TOPIC = directTopicName;

module.exports = {
  CLICK_KAFKA_TOPIC,
  TRANSACTION_TOPIC,
  GENERAL_TOPIC,
  DIRECT_ACTION_TOPIC
}