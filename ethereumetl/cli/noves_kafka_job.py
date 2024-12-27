import click
from ethereumetl.kafkapubsub import KafkaPublisher, KafkaSubscriber
from ethereumetl.cli.noves_job import noves_full_export
import uuid
from confluent_kafka import Message
import json
import os

KAFKA_URI = os.environ.get("KAFKA_URI", "localhost:19092")
PROVIDER_URI = os.environ.get("ETH_FULL_RPC_URI", "https://rpc-load-balancer.noves.fi/eth_full/")
DB_CONNSTRING = os.environ.get("DB_CONNSTRING", "postgresql://postgres:postgres@0.0.0.0:5432/ethereum")

KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_URI,
    "group.id": "ethereum-etl",
    "auto.offset.reset": "earliest",
}

CONSUME_TOPICS = [
    "eth-block-batch",
]
PRODUCE_TOPICS = [
    "eth-etl-block-batch-completed"
]

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
def run_worker():
    subscriber = KafkaSubscriber(KAFKA_CONFIG, CONSUME_TOPICS)
    publisher = KafkaPublisher(KAFKA_CONFIG)
    
    def process_message(message: Message):
        # {"start_block": 18203830, "end_block":18203835}
        m_val = message.value()
        block_batch_req = json.loads(m_val.decode("utf-8"))

        noves_full_export(
            block_batch_req["start_block"],
            block_batch_req["end_block"],
            batch_size=1,
            provider_uri=PROVIDER_URI,
            max_workers=1,
            db_connstring=DB_CONNSTRING,
            chain="ethereum",
        )
        
        publisher.publish(
            "eth-etl-block-batch-completed", str(uuid.uuid4()), m_val
        )

    subscriber.subscribe(process_message)
