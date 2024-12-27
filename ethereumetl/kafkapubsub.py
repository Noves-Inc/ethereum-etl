from confluent_kafka import Consumer, Producer, KafkaError, Message
from typing import Callable, Dict, List

class KafkaSubscriber:
    def __init__(self, kafka_config: Dict[str, str], topics: List[str]) -> None:
        """
        Initialize the KafkaSubscriber.

        :param kafka_config: Dictionary containing Kafka configuration.
        :param topics: List of topics to subscribe to.
        """
        self.consumer: Consumer = Consumer(kafka_config)
        self.topics: List[str] = topics
        self.consumer.subscribe(self.topics)

    def subscribe(self, process_message: Callable[[Message], None]) -> None:
        """
        Start consuming messages and apply the provided function on each message.

        :param process_message: Function to process a single message.
        """
        try:
            print(f"Subscribed to topics: {self.topics}")
            while True:
                msg: Message = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition for {msg.topic()} {msg.partition()}.")
                    else:
                        print(f"Consumer error: {msg.error()}")
                    continue

                process_message(msg)
        except KeyboardInterrupt:
            print("Subscription interrupted.")
        finally:
            self.consumer.close()

class KafkaPublisher:
    def __init__(self, kafka_config: Dict[str, str]) -> None:
        """
        Initialize the KafkaPublisher.

        :param kafka_config: Dictionary containing Kafka configuration.
        """
        self.producer: Producer = Producer({'bootstrap.servers': kafka_config['bootstrap.servers']})

    def publish(self, topic: str, key: bytes, value: bytes) -> None:
        """
        Publish a message to the specified topic.

        :param topic: The topic to publish the message to.
        :param key: The key of the message.
        :param value: The value of the message.
        """
        self.producer.produce(
            topic,
            key=key,
            value=value,
            on_delivery=lambda err, msg: print(f"Message delivered to {msg.topic()} [{msg.partition()}]" if err is None else f"Failed to deliver message: {err}")
        )
        self.producer.flush()
