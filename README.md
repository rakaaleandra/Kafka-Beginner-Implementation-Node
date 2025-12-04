# Kafka-Beginner-Implementation-Node

Create topic
```
docker exec kafka kafka-topics --create --topic weather-topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

List Topic
```
docker exec kafka kafka-topics --list  --bootstrap-server kafka:9092
```

buat kafka-nya

    docker compose up

producer-nya di port 3000

    npm run start:producer

consumer-nya di port 3001

    npm run start:consumer

ni yang bisa

    node producer_server.js
    node consumer_server.js