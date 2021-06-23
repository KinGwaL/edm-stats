import { dataStore, header } from 'index.js';
import { INTERACTION_STUDIO_ROOT } from '@mars/heroku-js-runtime-env';

export function directTrigger(data) {

    // Send Data to IS
    fetch(INTERACTION_STUDIO_ROOT, header(data)).then(function(response) {
      // Send Response Data To Dashboard
      fireGeneralTrigger(response);
    }, function(error) {
      // Store the failure data to dataStore and retry every 10 seconds
      dataStore({"data":data, "error":error});
    });
    
}