import { Kafka, Partitioners } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

const kafka = new Kafka({
  clientId: 'aqi-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
})

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
    
const run = async () => {
  try {
    await producer.connect()
    console.log('Producer connected to localhost:9092')

    
    setInterval(async () => {
        try {
        const aqiRecords = await getAirQualityData();
        if (!aqiRecords || aqiRecords.length === 0) return;
        
        // Send records in batches
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
    }, 3000)
  } catch (err) {
    console.error('Producer connection error:', err)
    process.exit(1)
  }
}

run().catch(console.error)