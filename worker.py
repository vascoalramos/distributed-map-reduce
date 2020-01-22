# coding: utf-8
from socket import socket, AF_INET, SOCK_STREAM

import string
import logging
import argparse
import json
import uuid
import re
import time
import locale
from functools import cmp_to_key

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('worker')

locale.setlocale(locale.LC_ALL, 'pt_PT.UTF-8')


class Worker:
    def __init__(self, hostname, port, id):
        # Socket used to connect to coordinator
        self.workerSocket = socket(AF_INET, SOCK_STREAM)
        self.workerSocket.settimeout(1000)
        self.workerSocket.connect((hostname, port))

        self.backupAddr = ()  # Address of backup, in case we want to communicate with it

        self.id = id

        self.msgBuffer = ""

    # Map function
    def work_map(self, blob):
        countMap = []

        words = self.tokenizer(blob)

        # Order Map Alphabetically
        words = sorted(words, key=cmp_to_key(locale.strcoll))

        # Create Map
        for word in words:
            if len(word) > 0:
                countMap.append((word.lower(), 1))

        return countMap

    # Reduce function
    def work_reduce(self, map1, map2):
        if len(map1) > 0:
            map1 = self.concatMap(map1)
        if len(map2) > 0:
            map2 = self.concatMap(map2)
        tempMap = []

        if len(map1) < len(map2):
            smallest = map1
            biggest = map2
        else:
            smallest = map2
            biggest = map1

        for word in smallest:
            index = self.binarySearch(biggest, 0, len(biggest)-1, word[0])
            if index != -1:
                foundItem = biggest[index]
                word = list(word)
                word[1] += foundItem[1]
                word = tuple(word)
                biggest.remove(foundItem)
            tempMap.append(tuple(word))

        if(len(biggest) != 0):
            tempMap.extend(biggest)
            self.mergeSort(tempMap)

        return tempMap

    # Basic binary search function
    def binarySearch(self, arr, l, r, x):
        if r >= l:
            mid = l + (r-l)//2
            mid_elem = arr[mid]

            if mid_elem[0] == x:
                return mid

            elif locale.strcoll(mid_elem[0], x) > 0:
                return self.binarySearch(arr, l, mid-1, x)

            else:
                return self.binarySearch(arr, mid+1, r, x)

        else:
            return -1

    # Basic merge sort function
    def mergeSort(self, arr):
        if len(arr) > 1:
            mid = len(arr)//2  # Finding the mid of the array
            L = arr[:mid]  # Dividing the array elements
            R = arr[mid:]  # into 2 halves

            self.mergeSort(L)  # Sorting the first half
            self.mergeSort(R)  # Sorting the second half

            i = j = k = 0

            # Copy data to temp arrays L[] and R[]
            while i < len(L) and j < len(R):
                if locale.strcoll(L[i][0], R[j][0]) < 0:
                    arr[k] = L[i]
                    i += 1
                else:
                    arr[k] = R[j]
                    j += 1
                k += 1

            # Checking if any element was left
            while i < len(L):
                arr[k] = L[i]
                i += 1
                k += 1

            while j < len(R):
                arr[k] = R[j]
                j += 1
                k += 1

    # Function used to remove (by concating) repeat words in the map
    def concatMap(self, Map):
        tempMap = []
        tempTuple = tuple(Map[0])
        for i in range(0, len(Map)-1):
            if Map[i+1][0] == tempTuple[0]:
                tmpList = list(tempTuple)
                tmpList[1] += 1
                tempTuple = tuple(tmpList)

            else:
                tempMap.append(tempTuple)
                tempTuple = tuple(Map[i+1])

        tempMap.append(tempTuple)

        return tempMap

    # Receive function. Used to receive and treat incoming data
    def receive(self, data):  # Receive function
        dataReceived = data.decode('UTF-8')

        if '\x04' not in dataReceived:  # If we haven't received a message with the break char
            self.msgBuffer += dataReceived  # Keep appending it to our msgBuffer
        else:
            # When we do receive a message with the break char we add it to the buffer but ignore the last char (cus its the break char)
            self.msgBuffer += dataReceived[:-1]
            self.handle()

    # Function used to send a register message to the coordinatorII
    def regWorker(self):
        # Send reg message
        regMsg = json.dumps({"task": "register", "id": str(self.id)})

        self.workerSocket.sendall(regMsg.encode(
            'UTF-8') + ('\x04').encode('UTF-8'))

    
    # Function used to handle incoming tasks
    def handle(self):
        msg = json.loads(self.msgBuffer)
        logger.info("Received a new task: %s", msg["task"])

        self.msgBuffer = ""

        if (msg["task"] == "reg_backup"):  # Register backup to worker
            self.backupAddr = msg['value']

        else:
            if (msg["task"] == "map_request"):  # Received a map request
                resultMap = self.work_map(msg['blob'])
                replyMsg = "map_reply"

            elif (msg["task"] == "reduce_request"):  # Received a reduce request
                countMap1 = msg["value"][0]
                countMap2 = msg["value"][1]
                resultMap = self.work_reduce(countMap1, countMap2)
                replyMsg = "reduce_reply"

            msg = json.dumps({"task": replyMsg, "value": resultMap})

            try:  # Try to send response
                self.workerSocket.sendall(msg.encode(
                    'UTF-8') + ('\x04').encode('UTF-8'))
            except:
                pass

    # Main communication loop
    def commLoop(self):
        self.regWorker()

        while True:

            # Receive work
            data = self.workerSocket.recv(4096)
            if not data:
                break
            self.receive(data)

    def tokenizer(self, txt):
        tokens = txt.lower()
        tokens = tokens.translate(str.maketrans('', '', string.digits))
        tokens = tokens.translate(str.maketrans('', '', string.punctuation))
        tokens = tokens.translate(str.maketrans('', '', '«»'))
        tokens = tokens.rstrip()
        return tokens.split()


def main(args):
    logger.info('Connecting %d to %s:%d', args.id, args.hostname, args.port)
    failCounter = 0

    while True:
        try:
            worker = Worker(args.hostname, args.port, args.id)
            failCounter = 0

            worker.commLoop()
        except:
            failCounter += 1
            logger.info(
                "Connection broken, trying to reconnect in 1 second...[Attempt %d]", failCounter)
            time.sleep(1)
            if failCounter >= 10:
                break
            pass

    logger.info("Attempted to connect too many times. Closing worker")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MapReduce worker')
    parser.add_argument('--id', dest='id', type=int,
                        help='worker id', default=0)
    parser.add_argument('--port', dest='port', type=int,
                        help='coordinator port', default=8765)
    parser.add_argument('--hostname', dest='hostname', type=str,
                        help='coordinator hostname', default='localhost')
    args = parser.parse_args()

    main(args)
