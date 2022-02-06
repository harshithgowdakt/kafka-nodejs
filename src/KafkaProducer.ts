import { Kafka, Producer } from "kafkajs";

class KafkaProducer {
    private kafka: Kafka;
    private producer: Producer;

    constructor() {
        this.kafka = new Kafka({
            clientId: "kafka-nodejs",
            brokers: ["localhost:29092"]
        });
        this.producer = this.Producer;
    }

    get Producer(): Producer {
        if (!this.producer) {
            this.producer = this.kafka.producer();
        }
        return this.producer;
    }

    async send() {
        try {
            await this.Producer.connect();
            await this.Producer.send({
                topic: "learning",
                messages: [{ key: "name", value: "Harshith" }, { key: "role", value: "SDE" }]
            });
            console.log("sent message successfully");
            await this.Producer.disconnect();
        } catch (error) {
            console.log("error while sending message", error);
        }
    }

    async sendBatch() {
        await this.Producer.connect();
        let topics = await this.Producer.sendBatch({
            topicMessages: [{
                topic: "learning",
                messages: [{ key: "name", value: "Gowda" }, { key: "role", value: "SDE" }]
            }, {
                topic: "learning",
                messages: [{ key: "name", value: "Gowda" }, { key: "role", value: "SDE" }]
            }]
        });
        await this.Producer.disconnect();
        return topics;
    }
}

(async function () {
    let client = new KafkaProducer();
    await client.send();
    await client.sendBatch();
})()
