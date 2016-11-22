#!/usr/bin/env python3

import json
import sys
import os
from socket import socket
from concurrent.futures import ThreadPoolExecutor as Executor

script = os.path.basename(__file__)


def handleConnection(connection, address):
    print('connecting', address)


def runServer(socket, ctx, pool):
    print('runServer', socket, pool)
    sock.bind((ctx['host'], ctx['port']))
    sock.listen(ctx['workers'])
    while True:
        connection, address = sock.accept()
        pool.submit(handleConnection, connection, address)


with open(sys.argv[1], 'r') as f:
    ctx = json.loads(f.read())[script]
    sock = socket()
    with Executor(max_workers=ctx['workers']) as pool:
        print('pool', pool)
        if ctx['type'] == 'server':
            runServer(sock, ctx, pool)
