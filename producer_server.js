import express from 'express';
import { Kafka, Partitioners } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();
const app = express();
const port = 3000;

app.use(express.json());

// 1. Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'my-producer-app',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer({ 
  createPartitioner: Partitioners.LegacyPartitioner 
});

const getAirQualityData = async () => {
  const apiKey = process.env.API_KEY;
  const id = 'A416908';
  const url = `https://api.waqi.info/feed/${id}/?token=${apiKey}`;

  const response = await fetch(url);
  const data = await response.json();

  if (data.status !== 'ok') {
    throw new Error('Failed to fetch air quality data');
  }

  const aqi = data.data.aqi;
  const city = data.data.city.name;
  const time = data.data.time.s;
  console.log(`City: ${city}, AQI: ${aqi}, Time: ${time}`);
  return [{
    city,
    aqi,
    time
  }];
}

const runProducer = async () => {
  // Connect once when the server starts
  await producer.connect();
  console.log('Producer connected to Kafka Broker');

  // Simple HTML form UI
  app.get('/', (req, res) => {
    res.send(`
      <html>
        <body>
          <h2>Kafka Producer</h2>
          <form method="POST" action="/produce">
            <input type="text" name="message" placeholder="Type your message" required />
            <button type="submit">Send</button>
          </form>
        </body>
      </html>
    `);
  });

  setInterval(async () => {
    try {
      const aqiRecords = await getAirQualityData();
      if (!aqiRecords || aqiRecords.length === 0) return;
      
      await producer.send({
        topic: 'aqi-topic',
        messages: aqiRecords.map(record => ({ 
          value: JSON.stringify(record) 
        })),
      })
      console.log(`Sent ${aqiRecords.length} aqi records`)

    } catch (err) {
      console.error('Send error:', err)
    }
    // producer.send({
    //   topic: topic,
    //   messages: [{ key: 'key1' , value: message }],
    //   // messages: [
    //   //   { key: 'key1', value: message, partition: 0 },
    //   //   { key: 'key2', value: timeMessage, partition: 1 }
    //   // ],
    // });
  }, 3000); // Keep the event loop alive for async operations


  // app.post('/produce', express.urlencoded({ extended: true }), async (req, res) => {
  //   try {
  //     const message = req.body.message || (req.body && req.body.message);
  //     const topic = 'notifications';

  //     if (!message) {
  //       return res.status(400).send('Message body is required');
  //     }

  //     await producer.send({
  //       topic: topic,
  //       messages: [{ value: message }],
  //     });

  //     console.log(`Sent: "${message}"`);
  //     // If form submission, show confirmation
  //     if (req.headers['content-type'] && req.headers['content-type'].includes('application/x-www-form-urlencoded')) {
  //       res.send(`<p>Message sent: ${message}</p><a href="/">Back</a>`);
  //     } else {
  //       res.send({ status: 'Message sent', message });
  //     }
  //   } catch (error) {
  //     console.error('Error sending message', error);
  //     res.status(500).send('Error sending message');
  //   }
  // });

  app.listen(port, () => {
    console.log(`Producer Server running on port ${port}`);
    console.log(`Send POST requests to http://localhost:${port}/produce with JSON body: { "message": "Hello" }`);
  });
};

runProducer().catch(console.error);
