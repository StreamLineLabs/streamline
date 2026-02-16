// Produce messages to Streamline using KafkaJS (standard Kafka client).

const { Kafka } = require("kafkajs");

const brokers = (process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:9092").split(",");
const kafka = new Kafka({ brokers });
const producer = kafka.producer();

async function main() {
  await producer.connect();

  for (let i = 0; i < 5; i++) {
    const msg = JSON.stringify({ event: "test", index: i });
    const result = await producer.send({
      topic: "demo",
      messages: [{ value: msg }],
    });
    const { partition, baseOffset } = result[0];
    console.log(`Sent to partition=${partition} offset=${baseOffset}: ${msg}`);
  }

  await producer.disconnect();
  console.log("Done! 5 messages produced to 'demo' topic.");
}

main().catch(console.error);
