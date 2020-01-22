# coding: utf-8
import socket

import logging
import json
import socket
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Backup')


class Backup:
    def __init__(self, coordHost, coordPort, datastore, host, port):
        # Socket used to connect to coordinator
        self.coordSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.coordSocket.connect((coordHost, coordPort))

        # Backup's address
        self.host = host
        self.port = port

        # Backup's rcv socket (used in case of communication with worker)
        self.backupSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.backupSocket.bind((host, port))

        # Backup values (synced with the coordinator's)
        self.datastore = datastore
        self.indexDatastore = 0
        self.maps = []

        # Msg buffer used to receive messages
        self.msgBuffer = ""

    # Receive function. Used to receive data
    def receive(self, connection, data):
        dataReceived = data.decode('UTF-8')

        if '\x04' not in dataReceived:  # If we haven't received a message with the break char
            self.msgBuffer += dataReceived  # Keep appending it to our msgBuffer

        else:  # When we receive a message with a break character

            splitBuf = dataReceived.split('\x04')
            auxBuf = self.msgBuffer + splitBuf[0]
            self.handle(auxBuf)
            for i in splitBuf[1:]:
                if (('{'in i) and ('}' in i)):
                    self.handle(i)
                else:
                    self.msgBuffer = i

    # Function used to send a reg message to the coordinator
    def regBackup(self):
        regMsg = json.dumps(
            {"task": "reg_backup", 'addr': (self.host, self.port)})
        logger.debug((self.host, self.port))
        self.coordSocket.sendall(regMsg.encode(
            'UTF-8') + ('\x04').encode('UTF-8'))

    # Function used to handle incoming requests
    def handle(self, data):
        try:
            msg = json.loads(data)
            logger.info('Handling task %s', msg["task"])
            if msg['task'] == 'update':  # Sync datastore/datastore index
                self.maps = msg['value']
                self.indexDatastore = msg['index']
        except:
            pass

    # Funciton that initializes the whole backup process (registers backup then starts receiving)
    def start_backup(self):
        self.regBackup()
        while True:
            data = self.coordSocket.recv(4096)
            if not data:
                break
            self.receive(self.coordSocket, data)

        logger.info("Backup closing")
        self.coordSocket.close()
