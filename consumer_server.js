const express = require('express');
const { Kafka } = require('kafkajs');
let cassandra = require('cassandra-driver');

const app = express();
const port = 3001;

// 1. Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'my-consumer-app',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'test-group-1' });

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'notifications', fromBeginning: true });

  console.log('Consumer connected and subscribed');

  // Store the latest message
  let latestMessage = '';

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      latestMessage = message.value.toString();
      console.log(`CONSUMER RECEIVED: ${prefix} - ${latestMessage}`);
    },
  });

  // Simple UI: message box and button
  app.get('/', (req, res) => {
    res.send(`
      <html>
        <body>
          <h2>Kafka Consumer</h2>
          <div>
            <label>Latest Message:</label>
            <div id="msgbox" style="border:1px solid #ccc;padding:10px;min-height:30px;">${latestMessage || 'No message yet.'}</div>
            <button onclick="consumeMsg()">Consume</button>
          </div>
          <script>
            function consumeMsg() {
              fetch('/latest')
                .then(r => r.json())
                .then(d => {
                  document.getElementById('msgbox').innerText = d.message || 'No message yet.';
                });
            }
          </script>
        </body>
      </html>
    `);
  });

  // Endpoint to get the latest message
  app.get('/latest', (req, res) => {
    res.json({ message: latestMessage });
  });

  app.listen(port, () => {
    console.log(`Consumer Server running on port ${port}`);
  });
};

runConsumer().catch(console.error);
