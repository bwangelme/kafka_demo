import argparse
import sys

import producer
import consumer


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title='commands', dest='subparser_command')

    for mod in [producer, consumer]:
        subparser = subparsers.add_parser(name=mod.COMMAND_NAME,
                                         help=mod.COMMAND_DESCRIPTION)
        mod.populate_argument_parser(subparser)
        subparser.set_defaults(func=mod.main)

    argv = sys.argv[1:] or ['--help']
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    main()
