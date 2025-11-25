import express from 'express';
import { Kafka, Partitioners } from 'kafkajs';
import { fetchWeatherApi } from "openmeteo";

const params = {
	latitude: 52.52,
	longitude: 13.41,
	hourly: "temperature_2m",
};
const url = "https://api.open-meteo.com/v1/forecast";
const responses = await fetchWeatherApi(url, params);

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

// Process first location. Add a for-loop for multiple locations or weather models
const response = responses[0];

// Attributes for timezone and location
const latitude = response.latitude();
const longitude = response.longitude();
const elevation = response.elevation();
const utcOffsetSeconds = response.utcOffsetSeconds();

console.log(
	`\nCoordinates: ${latitude}°N ${longitude}°E`,
	`\nElevation: ${elevation}m asl`,
	`\nTimezone difference to GMT+0: ${utcOffsetSeconds}s`,
);

const hourly = response.hourly();

// Note: The order of weather variables in the URL query and the indices below need to match!
const weatherData = {
	hourly: {
		time: Array.from(
			{ length: (Number(hourly.timeEnd()) - Number(hourly.time())) / hourly.interval() }, 
			(_, i) => new Date((Number(hourly.time()) + i * hourly.interval() + utcOffsetSeconds) * 1000)
		),
		temperature_2m: hourly.variables(0).valuesArray(),

	},
};

// The 'weatherData' object now contains a simple structure, with arrays of datetimes and weather information
// console.log("\nHourly data:\n", weatherData.hourly)

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

  setInterval(() => {
    // Dapatkan waktu sekarang (sesuai timezone lokasi dari API)
    const now = new Date(Date.now() + utcOffsetSeconds * 1000);
    // const now = new Date(2025, 2, 10, 2, 30);
  
    // Cari index timestamp hourly yang cocok dengan jam sekarang
    const times = weatherData.hourly.time;
    const temps = weatherData.hourly.temperature_2m;
  
    // Cari index waktu precision 1 jam
    const indexNow = times.findIndex(t => 
        t.getHours() === now.getHours() &&
        t.getDate() === now.getDate()
    );
  
    if (indexNow !== -1) {
      console.log("Current time:", times[indexNow]);
      console.log("Current temperature:", temps[indexNow], "°C");
      const message = temps[indexNow].toString();
      const topic = 'notifications';
      producer.send({
        topic: topic,
        messages: [{ value: message }],
      });
    } else {
      console.log("Data untuk waktu sekarang tidak ditemukan.");
    }
  }, 1000); // Keep the event loop alive for async operations

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

      console.log(`Sent: "${message}"`);
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
    console.log(`Producer Server running on port ${port}`);
    console.log(`Send POST requests to http://localhost:${port}/produce with JSON body: { "message": "Hello" }`);
  });
};

runProducer().catch(console.error);
