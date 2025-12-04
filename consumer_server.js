import express from 'express';
import { Kafka, Partitioners } from 'kafkajs';
import cassandra from 'cassandra-driver';

const app = express();
const port = 3001;

// 1. Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'my-consumer-app',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'test-group-1' });

// 2. Initialize Cassandra Client
const cassandraClient = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1',
  protocolOptions: { port: 9042 },
  keyspace: 'kafka_data',
});

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
      const key = message.key?.toString();
      const value = message.value?.toString();
      const offset = message.offset;
      const timestamp = message.timestamp;
      console.log(`CONSUMER RECEIVED: ${prefix} - ${latestMessage} - ${key}`);
      const query = `
        INSERT INTO messages (id, topic, partition, offset, key, value, timestamp)
        VALUES (uuid(), ?, ?, ?, ?, ?, ?)
      `;
      await cassandraClient.execute(query, [
        topic,
        partition,
        offset,
        key,
        value,
        timestamp
      ], { prepare: true });
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
