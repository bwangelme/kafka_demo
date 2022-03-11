import msgpack
from confluent_kafka import Producer

COMMAND_NAME = 'prod'
COMMAND_DESCRIPTION = 'kafka producer'


def populate_argument_parser(subparser):
    subparser.add_argument('-n', '--number', help='msg count', type=int, default=10)


def main(args):
    p = Producer({
        'bootstrap.servers': 'localhost:9092',
    })
    val = msgpack.dumps({
        'name': 'xiaohei'
    })
    for i in range(args.number):
        p.produce('lazy', value=val)
        p.poll(0)

    p.flush(10)
