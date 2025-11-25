const express = require('express');
const { Kafka, Partitioners } = require('kafkajs');

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

const runProducer = async () => {
  // Connect once when the server starts
  await producer.connect();
  console.log('✅ Producer connected to Kafka Broker');

  // Define the route to send messages
  app.post('/produce', async (req, res) => {
    try {
      const { message } = req.body;
      const topic = 'notifications';

      if (!message) {
        return res.status(400).send('Message body is required');
      }

      await producer.send({
        topic: topic,
        messages: [{ value: message }],
      });

      console.log(`📤 Sent: "${message}"`);
      res.send({ status: 'Message sent', message });

    } catch (error) {
      console.error('Error sending message', error);
      res.status(500).send('Error sending message');
    }
  });

  app.listen(port, () => {
    console.log(`🚀 Producer Server running on port ${port}`);
    console.log(`👉 Send POST requests to http://localhost:${port}/produce with JSON body: { "message": "Hello" }`);
  });
};

runProducer().catch(console.error);
