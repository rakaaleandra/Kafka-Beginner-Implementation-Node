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

  // Accept both JSON and form submissions
  app.post('/produce', express.urlencoded({ extended: true }), async (req, res) => {
    try {
      // Accept from either JSON or form
      const message = req.body.message || (req.body && req.body.message);
      const topic = 'notifications';

      if (!message) {
        return res.status(400).send('Message body is required');
      }

      await producer.send({
        topic: topic,
        messages: [{ value: message }],
      });

      console.log(`📤 Sent: "${message}"`);
      // If form submission, show confirmation
      if (req.headers['content-type'] && req.headers['content-type'].includes('application/x-www-form-urlencoded')) {
        res.send(`<p>Message sent: ${message}</p><a href="/">Back</a>`);
      } else {
        res.send({ status: 'Message sent', message });
      }
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
