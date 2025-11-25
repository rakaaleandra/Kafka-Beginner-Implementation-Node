const express = require('express');
const { Kafka } = require('kafkajs');

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

  console.log('✅ Consumer connected and subscribed');

  // Start the Kafka Consumer
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`📥 CONSUMER RECEIVED: ${prefix} - ${message.value.toString()}`);
    },
  });

  // Start the Express server just to keep the process "listening" on a port
  // and to provide a status endpoint
  app.get('/', (req, res) => {
    res.send('Consumer is running and listening to Kafka...');
  });

  app.listen(port, () => {
    console.log(`👂 Consumer Server running on port ${port}`);
  });
};

runConsumer().catch(console.error);
