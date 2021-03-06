#!/usr/bin/env python3

import json
import bson
import sys
import os
from socket import socket
from time import sleep
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor as Executor


parser = argparse.ArgumentParser(description='basic client server app')
parser.add_argument('--host', required=True)
parser.add_argument('--port', type=int, required=True)
parser.add_argument('-v', '--verbose', action='count')
parser.add_argument('-n', '--workers', type=int, default=os.cpu_count())
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    '-s',
    '--server',
    action='store_true')
group.add_argument(
    '-u',
    '--client',
    action='store_true')


args = parser.parse_args()

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(threadName)s $$ %(message)s'

LOGGING_LEVEL = logging.ERROR
if args.verbose is None:
    args.verbose = 0
if args.verbose == 1:
    LOGGING_LEVEL = logging.WARNING
elif args.verbose == 2:
    LOGGING_LEVEL = logging.INFO
elif args.verbose >= 3:
    LOGGING_LEVEL = logging.DEBUG

logging.basicConfig(level=LOGGING_LEVEL, format=LOGGING_FORMAT)


class argdict(dict):
    __setattr__ = dict.__setitem__
    __getattr__ = dict.__getitem__


def initialize(msg, **kwds):
    super(msg.__class__, msg).__init__(**kwds)


def serialize(msg):
    data = bson.dumps(msg)
    return len(data).to_bytes(4, 'big') + data


def send(msg, sock):
    sock.sendall(msg.serialize())


class BaseMessage(type):

    def __new__(cls, name, bases, mems, **kwds):
        mems['__init__'] = initialize
        mems['serialize'] = serialize
        mems['send'] = send
        return super(BaseMessage, cls).__new__(cls, name, bases, mems)

    def __init__(cls, name, bases, mems, category):
        super(BaseMessage, cls).__init__(name, bases, mems)
        cls.category = category

    def __call__(cls, *args, **kwds):
        kwds['category'] = cls.category.value
        return super(BaseMessage, cls).__call__(*args, **kwds)

from enum import Enum, unique


@unique
class category(str, Enum):
    REQ = 'REQ'
    RESP = 'RESP'
    IND = 'IND'

Message = {e: BaseMessage(e.value, (argdict,), {}, category=e)
           for e in category}


def handleMessage(connection, address, msg):
    logging.info(
        'message from {}:{} {}'.format(
            address[0], address[1], msg['payload']))
    sleep(3.0)

sock = socket()
with Executor(max_workers=args.workers) as pool:
    logging.info('{} workers started'.format(args.workers))

    def makeMessage(dct):
        return Message[dct['category']](payload=dct['payload'])

    def sendMessage(msg):
        logging.info(
            'sending to {}:{} {} {}'.format(
                args.host, args.port, msg['category'], msg['payload']))
        try:
            msg.send(sock)
        except Exception as e:
            logging.error('caught exception: {0}'.format(e))

    def runClient():
        logging.info('running in client mode')
        inmsgs = json.loads(sys.stdin.read())
        logging.info('connecting to {}:{}'.format(args.host, args.port))
        sock.connect((args.host, args.port))
        for msg in inmsgs:
            ftr = pool.submit(sendMessage, makeMessage(msg))
        logging.info('sent all messages')

    def handleConnection(connection, address):
        def readUntil(data, size):
            while len(data) < size:
                data += connection.recv(1024)
            return data[:size], data[size:]

        def readMessage(data):
            try:
                msg = bson.loads(data)
            except Exception as e:
                logging.error('exception: {}'.format(e))
            return msg

        def handleParse(data):
            size = int.from_bytes(data[:4], 'big')
            data, buff = readUntil(data[4:], size)
            ftr = pool.submit(readMessage, data)
            ftr.add_done_callback(
                lambda f: handleMessage(connection, address, f.result()))
            return buff

        def handleRead(data):
            while len(data) >= 4:
                data = handleParse(data)
                data += connection.recv(1024)

        logging.info(
            'connection from {}:{} accepted'.format(
                address[0], address[1]))
        data = connection.recv(1024)
        pool.submit(handleRead, data)

    def runServer():
        logging.info('running in server mode')
        logging.info('binding to {}:{}'.format(args.host, args.port))
        sock.bind((args.host, args.port))
        sock.listen(args.workers)
        while True:
            connection, address = sock.accept()
            pool.submit(handleConnection, connection, address)

    if args.server:
        runServer()
    elif args.client:
        runClient()
