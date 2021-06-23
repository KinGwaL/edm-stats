import { consumer } from '../index.js';
import { directTrigger, transactionTrigger } from '../trigger';
import { TRANSACTION_TOPIC } from '@mars/heroku-js-runtime-env';

// Listening + Routing
consumer.on('data', function(data) {
    const json = JSON.parse(data.value.toString());

    // Topic Switching
    switch (json.topic) {
      case TRANSACTION_TOPIC: // Transaction Topic
        transactionTrigger(json);
        break;

      default: // Generic Topic
        directTrigger(json);
        break;
    }
});
