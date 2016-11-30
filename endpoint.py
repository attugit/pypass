#!/usr/bin/env python3

import json
import bson
import sys
import os
from socket import socket
from time import sleep
import argparse
import logging
#from concurrent.futures import ThreadPoolExecutor as Executor
from concurrent.futures import ThreadPoolExecutor as Executor


parser = argparse.ArgumentParser(description='basic client server app')
parser.add_argument('--host', required=True)
parser.add_argument('--port', type=int, required=True)
parser.add_argument('-v', '--verbose', action='count')
parser.add_argument('-n', '--workers', type=int, default=os.cpu_count())
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument(
    '-s',
    '--serve',
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

from enum import Enum, unique


@unique
class message_type(str, Enum):
    REQ = 'REQ'
    RESP = 'RESP'
    ACK = 'ACK'
    IND = 'IND'
    PUB = 'PUB'


class BaseMessage(argdict):

    def __init__(self, mtype, payload):
        super(BaseMessage, self).__init__()
        self.type = mtype
        self.payload = payload


class Indicator(BaseMessage):

    def __init__(self, payload):
        super(Indicator, self).__init__(message_type.IND, payload=payload)


def handleMessage(address, data):
    msg = bson.loads(data)
    logging.critical(
        'message from {}:{} {}'.format(
            address[0], address[1], msg))
    sleep(1.0)


def handleConnection(connection, address, pool):
    logging.debug(
        'connection from {}:{} accepted'.format(
            address[0], address[1]))
    data = connection.recv(1024)
    logging.debug('received {} bytes'.format(len(data)))
    while len(data) >= 4:
        size = int.from_bytes(data[:4], 'big')
        logging.debug(
            'message size {}, buffer size {}'.format(
                size, len(data)))
        if len(data) >= size + 4:
            pool.submit(handleMessage, address, data[4:size + 4])
            data = data[size + 4:]
        data += connection.recv(1024)
    logging.debug('end of listening')


def runServer(sock, pool):
    logging.info('binding to {}:{}'.format(args.host, args.port))
    sock.bind((args.host, args.port))
    sock.listen(args.workers)
    while True:
        connection, address = sock.accept()
        pool.submit(handleConnection, connection, address, pool)


def sendMessage(sock, idx, total, msg):
    logging.info('sending msg {} of {}: {}'.format(idx, total, msg))
    try:
        ind = Indicator(msg)
        data = bson.dumps(ind)
        sock.sendall(len(data).to_bytes(4, 'big') + data)
    except Exception as e:
        logging.error('caught exception: {0}'.format(e))
    else:
        sleep(2.0)


def runClient(sock, pool):
    inmsgs = json.loads(sys.stdin.read())
    logging.info('connecting to {}:{}'.format(args.host, args.port))
    sock.connect((args.host, args.port))
    for idx, msg in enumerate(inmsgs):
        pool.submit(sendMessage, sock, idx, len(inmsgs), msg)
    logging.debug('sent all messages')

sock = socket()
with Executor(max_workers=args.workers) as pool:
    logging.debug('{} workers started'.format(args.workers))
    if args.serve:
        logging.info('running in server mode')
        runServer(sock, pool)
    elif args.client:
        logging.info('running in client mode')
        runClient(sock, pool)
