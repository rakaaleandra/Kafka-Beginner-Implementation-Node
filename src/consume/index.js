import { Kafka } from 'kafkajs'

const kafka = new Kafka({
  clientId: 'aqi-app',
  brokers: ['localhost:9092'],
})

const consumer = kafka.consumer({ groupId: 'aqi-group' })
const run = async () => {
    try{
        await consumer.connect()
        console.log('Consumer connected to localhost:9092')
        try{
            await consumer.subscribe({ topic: 'aqi-topic' })
            await consumer.run({
              eachMessage: async ({ topic, partition, message }) => {
                const record = JSON.parse(message.value.toString());
                
                console.log('\n==================================');
                console.log(`Received message from: ${topic}`);
                console.log(`Partition: ${partition}`);
                console.log(`City: ${record.city}`);
                console.log(`AQI: ${record.aqi}`);
                console.log(`Time: ${record.time}`);
                console.log('==================================');

             },
            })
        } catch (error) {
            console.error('Error subscribing to topic:', error)
        }
    } catch (error) {
        console.error('Error connecting consumer:', error)
    }
}

run().catch(console.error)
