import { Kafka, Admin } from "kafkajs";

class KafkaAdmin {
    private kafka: Kafka;
    private admin: Admin;

    constructor() {
        this.kafka = new Kafka({
            clientId: "kafka-nodejs",
            brokers: ["localhost:29092"]
        });
        this.admin = this.kafka.admin();
    }

    get Admin(): Admin {
        if (!this.admin) {
            this.admin = this.kafka.admin();
        }
        return this.admin;
    }

    async createTopic() {
        try {
            await this.Admin.connect();
            await this.Admin.createTopics({
                topics: [{
                    topic: "learning",
                    numPartitions: 2
                }]
            });
            console.log("created topic successfully");
            await this.Admin.disconnect();
        } catch (error) {
            console.log("error while connecting to kafka", error);
        }
    }

    async listTopics() {
        await this.Admin.connect();
        let topics = await this.Admin.listTopics();
        await this.Admin.disconnect();
        return topics;
    }
}


let client = new KafkaAdmin();
client.createTopic();
client.listTopics().then(topics => console.log(topics));