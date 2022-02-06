import { Consumer, Kafka } from "kafkajs";

class KafkaConsumer {
    private kafka: Kafka;
    private consumer: Consumer;

    constructor() {
        this.kafka = new Kafka({
            clientId: "kafka-nodejs",
            brokers: ["localhost:29092"]
        });
        this.consumer = this.Consumer;
    }

    get Consumer(): Consumer {
        if (!this.consumer) {
            this.consumer = this.kafka.consumer({ groupId: "test-consumer" });
        }
        return this.consumer;
    }

    async subscribe() {
        try {
            await this.Consumer.connect();
            await this.Consumer.subscribe({
                topic: "learning",
                fromBeginning: true
            });
            console.log("sent message successfully");
        } catch (error) {
            console.log("error while sending message", error);
        }
    }

    async run() {
        await this.Consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log({
                    topic: topic,
                    partition: partition,
                    key: message.key.toString(),
                    value: message?.value?.toString(),
                    headers: message.headers,
                })
            }
        });
    }
}

(async function () {
    let client = new KafkaConsumer();
    await client.subscribe();
    await client.run();
})()
