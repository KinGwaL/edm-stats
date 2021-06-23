import { directTrigger } from '../trigger';

export function transactionTrigger(data) {

    // Restructure Data
    const currencyData = data["Request"]["ForeignCurry"];
    const priceData = data["Response"]["CalculatedHKDAmount"];
    const json = {
      "action": `CNCBI - FX Transaction - ${currencyData}`,
      "orderId": uuid.v1(),
      "currency": currencyData,
      "lineItems":[{
        "_id":currencyData,
        "price": priceData,
        "quantity": 1
      }]
    };

    // Send Data to IS
    directTrigger(json);
  }