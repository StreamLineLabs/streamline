// Consume messages from Streamline using KafkaJS (standard Kafka client).

const { Kafka } = require("kafkajs");

const brokers = (process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092").split(",");
const kafka = new Kafka({ brokers });
const consumer = kafka.consumer({ groupId: "kafkajs-example" });

async function main() {
  await consumer.connect();
  await consumer.subscribe({ topic: "demo", fromBeginning: true });

  let count = 0;
  const timeout = setTimeout(async () => {
    console.log(`Done! Consumed ${count} messages.`);
    await consumer.disconnect();
    process.exit(0);
  }, 5000);

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const value = message.value.toString();
      console.log(`  offset=${message.offset} partition=${partition} value=${value}`);
      count++;
    },
  });
}

main().catch(console.error);
