# coding: utf-8
from socket import socket, AF_INET, SOCK_STREAM
from backup import Backup

import csv
import logging
import argparse
import json
import asyncio
import sys
import queue

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Coordinator')


# Asyncio class (we used callback asyncio)
class EchoProtocol(asyncio.BaseProtocol):

    def __init__(self, coordinator):
        self.coordinator = coordinator

    def connection_made(self, transport):
        self.transport = transport
        self.socket = transport.get_extra_info('socket')
        self.addr = transport.get_extra_info('peername')
        logger.info('New connection established: %s', self.addr)

    def data_received(self, data):
        self.coordinator.receive(self.socket, data)

    def eof_received(self):
        self.coordinator.redistributeWork(self.socket)


# Coordinator class
class Coordinator:
    def __init__(self, datastore, datastoreIndex=0, maps=[], msgBuffer=""):
        self.datastore = datastore  # Array with all of the blobs

        if len(self.datastore) == 1:
            self.singleBlob = True  # Control variable used to force a reduce if we only have 1 blob
        else:
            self.singleBlob = False

        # Index that points to what blob we're treating
        self.datastoreIndex = datastoreIndex

        self.lostWork = queue.Queue()  # Queue with work lost when a worker crashes

        self.sentWork = {}  # Dictionary with all work messages sent and to whom

        self.workers = {}  # Dictionary with worker's id-connection

        self.tasksCount = 0

        self.msgBuffer = msgBuffer  # Incoming message buffer

        self.maps = maps  # Maps we're treating

        logger.debug("Map : %s", self.maps)
        logger.debug(self.datastoreIndex)

        # Backup:
        # Backup connection (Backup-Coordinator address)
        self.backupConn = None
        self.backupAddr = ()  # Backup address (Backup socket address)

    # Receive function. Used to receive and treat incoming data
    def receive(self, connection, data):
        dataReceived = data.decode('UTF-8')

        if '\x04' not in dataReceived:  # If we haven't received a message with the break char
            self.msgBuffer += dataReceived  # Keep appending it to our msgBuffer

        else:
            splitBuf = dataReceived.split('\x04')
            auxBuf = self.msgBuffer + splitBuf[0]
            self.msgBuffer = splitBuf[1]

            self.handle(connection, auxBuf)

    # Send function. Used to send messages
    def send(self, connection, data):
        connection.sendall(data.encode(
            'UTF-8') + ('\x04').encode('UTF-8'))
        self.sentWork[connection] = data

################ SYNC FUNCS ############################################
    # Sends current maps and index values to backup
    def syncData(self):
        msg = {"task": "update", "value": self.maps,
               "index": self.datastoreIndex}
        updateMsg = json.dumps(msg)
        try:
            self.backupConn.sendall(updateMsg.encode(
                'UTF-8') + ('\x04').encode('UTF-8'))
        except OSError:
            pass
#######################################################################

    # Function used to register work that had been sent to a worker that died
    def redistributeWork(self, connection):
        if (connection != self.backupConn) and (connection in list(self.workers.values())):
            for workerID, workerConn in self.workers.items():
                if workerConn == connection:
                    del self.workers[workerID]
                    self.lostWork.put(self.sentWork[connection])
                    break

    # Function used to register a new worker
    def regWorker(self, connection, workerID):

        self.workers[workerID] = connection  # Add it to our worker dict
        logger.info('Worker registered with id %s', workerID)

        if self.backupConn is not None:  # If we have a backup, give the worker a viable address to connect to it
            workMsg = json.dumps(
                {"task": "reg_backup", "value": self.backupAddr})
            self.send(connection, workMsg)

        self.giveWork(connection)

    # Function used to write a csv with the resulting final map
    def writeToCSV(self):
        with args.out as f:
            csv_writer = csv.writer(
                f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for w, c in self.maps[0]:
                csv_writer.writerow([w, c])

    # Function used to give work to a worker
    def giveWork(self, connection):
        if (len(self.maps) < 2):  # If we only have 1 map in our maps
            # And still have blobs in the datastore, get a new one and send it to be mapped (either from the lost works or the datastore)
            if (self.datastoreIndex < len(self.datastore)):
                if (self.lostWork.qsize() == 0):
                    workMsg = json.dumps(
                        {"task": "map_request", "blob": self.datastore[self.datastoreIndex]})
                    self.send(connection, workMsg)

                    self.datastoreIndex += 1

                    if self.backupConn is not None:
                        self.syncData()  # Update backup data

                else:
                    workMsg = self.lostWork.get()
                    self.send(connection, workMsg)

                    if self.backupConn is not None:
                        self.syncData()  # Update backup data

            else:
                if self.backupConn is not None:
                    self.syncData()  # Update backup data

                self.tasksCount += 1

                logger.debug(str(self.tasksCount) + " | " + str(len(self.workers)))
                if (self.tasksCount == len(self.workers)):
                    if self.singleBlob == True:  # In case we only have 1 blob we have to force the reduce!
                        self.singleBlob = False
                        self.tasksCount -= 1

                        self.maps.append([])
                        workMsg = json.dumps(
                            {"task": "reduce_request", "value": [self.maps[0], self.maps[1]]})
                        self.send(connection, workMsg)

                        del self.maps[1]
                        del self.maps[0]

                    else:
                        logger.info('Map complete: %s', self.maps)
                        if self.backupConn is not None:
                            self.syncData()  # Update backup data

                        self.writeToCSV()  # Store final histogram into a CSV file

                        sys.exit()

                        

        else:  # Otherwise, reduce the 2 maps we've got OR send work that had been lost
            if (self.lostWork.qsize() == 0):
                workMsg = json.dumps(
                    {"task": "reduce_request", "value": [self.maps[0], self.maps[1]]})
                self.send(connection, workMsg)

                if self.backupConn is not None:
                    self.syncData()  # Update backup data

                del self.maps[1]
                del self.maps[0]

            else:
                workMsg = self.lostWork.get()
                self.send(connection, workMsg)

                if self.backupConn is not None:
                    self.syncData()  # Update backup data

    # Function used to handle incoming requests
    def handle(self, connection, data):
        msg = json.loads(data)
        logger.info('Handling task %s', msg["task"])

        if msg['task'] == 'register':  # Register new worker
            self.regWorker(connection, msg['id'])

        elif msg['task'] == 'reg_backup':  # Register new backup
            self.backupConn = connection
            self.backupAddr = msg['addr']

            workMsg = json.dumps(
                {"task": "reg_backup", "value": self.backupAddr})

            for workerID, workerConn in self.workers.items():  # Register new backup at all workers
                self.send(workerConn, workMsg)

            logger.debug("Registered backup!")

        elif msg['task'] == 'map_reply' or msg['task'] == 'reduce_reply':  # Receive a work reply
            self.maps.append(msg["value"])
            self.giveWork(connection)


# Main process initializing function
async def main(args):
    datastore = []

    # Build blobs
    with args.file as f:
        while True:
            blob = f.read(args.blob_size)
            if not blob:
                break
            # This loop is used to not break word in half
            while not str.isspace(blob[-1]):
                ch = f.read(1)
                if not ch:
                    break

                blob += ch

            logger.debug('Blob: %s\n\n', blob)
            datastore.append(blob)

    #############################################

    # Create Coordinator
    coordinator = Coordinator(datastore)
    failCounter = 0

    try:  # Its a coordinator!
        loop = asyncio.get_event_loop()
        server = await loop.create_server(lambda: EchoProtocol(coordinator), "127.0.0.1", args.port)
        logger.info("Coordinator created!")
        await server.serve_forever()

    except:  # Its a backup!
        port = args.port + 1  # Backup port, used to communicate with workers
        while True:
            try:
                backup_coord = Backup(
                    "127.0.0.1", args.port, datastore, "127.0.0.1", port)
                failCounter = 0
                break
            except:
                failCounter += 1
                port += 1
                if failCounter >= 10:
                    break
                pass

        logger.info("Backup created!")
        backup_coord.start_backup()

        # When the coordinator dies, backup becomes the new coordinator by launching a server
        coordinator = Coordinator(
            datastore, backup_coord.indexDatastore, backup_coord.maps)

        loop = asyncio.get_event_loop()
        server = await loop.create_server(lambda: EchoProtocol(coordinator), "127.0.0.1", args.port)
        logger.info("Coordinator created!")
        await server.serve_forever()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce Coordinator')
    parser.add_argument('-p', dest='port', type=int,
                        help='coordinator port', default=8765)
    parser.add_argument('-f', dest='file',
                        type=argparse.FileType('r'), help='file path')
    parser.add_argument('-o', dest='out', type=argparse.FileType('w',
                                                                 encoding='UTF-8'), help='output file path', default='output.csv')
    parser.add_argument('-b', dest='blob_size', type=int,
                        help='blob size', default=1024)
    args = parser.parse_args()

    loop = asyncio.get_event_loop()

    loop.run_until_complete(main(args))

    loop.close()