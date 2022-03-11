from confluent_kafka import Consumer
import msgpack
import log

COMMAND_NAME = 'consumer'
COMMAND_DESCRIPTION = 'kafka consumer'


logger = log.setup_logger(__name__)


def populate_argument_parser(subparser):
    pass


def main(args):
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        # 设置新的 group 从哪里开始读 msg, smallest, earliest, beginning 表示从头开始读
        # 'auto.offset.reset': 'smallest',
        'group.id': 'group.lazy',
    })
    c.subscribe(['lazy'])
    logger.info("Start Consumer")
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.info("Consumer error: {}".format(msg.error()))
            continue

        v = msg.value()
        val = msgpack.loads(v)
        logger.info('Received message: {}/{}'.format(msg.offset(), val['name']))

    c.close()
