#!/usr/bin/env python3

import json
import bson
import sys
import os
from socket import socket
from concurrent.futures import ThreadPoolExecutor as Executor

script = os.path.basename(__file__)


def handleConnection(connection, address):
    print('connecting', address)
    data = connection.recv(1024)
    print(bson.loads(data))


def runServer(sock, ctx, pool):
    print('runServer', sock, pool)
    sock.bind((ctx['host'], ctx['port']))
    sock.listen(ctx['workers'])
    while True:
        connection, address = sock.accept()
        pool.submit(handleConnection, connection, address)


def runClient(sock, ctx, pool):
    print('runClient', sock, pool)
    inmsg = json.loads(sys.stdin.read())
    sock.connect((ctx['host'], ctx['port']))
    sock.sendall(bson.dumps(inmsg))

with open('config.json', 'r') as f:
    ctx = json.loads(f.read())[script]
    sock = socket()
    with Executor(max_workers=ctx['workers']) as pool:
        print('pool', pool)
        if ctx['type'] == 'server':
            runServer(sock, ctx, pool)
        elif ctx['type'] == 'client':
            runClient(sock, ctx, pool)
