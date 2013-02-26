import argparse
import heapq
import json as js
import logging
import logging.handlers as loghandlers
import itertools
import os
import subprocess
import sys
import time
import uuid

import zmq

from makeflow import MakeflowService

REQUEST = 'request'
DATA = 'data'

ERROR = "-1"

workflows = {}
jex_queue = []
counter = itertools.count()
encoder = js.JSONEncoder()
workflow_provider = MakeflowService()

# Setup Logging
if os.path.exists("logging.conf"):
   logging.config.fileConfig("logging.conf") 
   logger = logging.getLogger('yerba')
else:
    logger = logging.getLogger('yerba')
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] %(message)s',
                            datefmt='%m/%d/%Y %I:%M:%S')

    filehandler = loghandlers.TimedRotatingFileHandler('yerba.log', 'midnight')
    filehandler.setLevel(logging.DEBUG)
    filehandler.setFormatter(fmt)

    streamhandler = logging.StreamHandler()
    streamhandler.setLevel(logging.INFO)
    streamhandler.setFormatter(fmt)

    logger.addHandler(filehandler)
    logger.addHandler(streamhandler)

def schedule_workflow(workflow):
    id = str(uuid.uuid4().int)
    makeflow = workflow_provider.create_workflow(workflow)
    count = next(counter)

    entry = (0, count, [id, makeflow])
    workflows[id] = entry
    heapq.heappush(jex_queue, entry)
    return id

def fetch_workflow():
    (priority, count, workflow) = heapq.heappop(jex_queue)
    (id, makeflow) = workflow
    return makeflow

def get_status(id):
    if id in workflows:
        (priority, count, workflow) = workflows[id]
        (id, makeflow) = workflow
        print makeflow
        status = workflow_provider.get_status(makeflow)
        return encoder.encode(status)
    else:
        return encoder.encode({ "status" : "NOT_FOUND"})
  
def dispatch(request, data):
    if request == "get_status":
        return get_status(data)
    elif request == "schedule":
        return schedule_workflow(data)

def listen_forever(port):
    context = zmq.Context()

    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)

    jd = js.JSONDecoder()
    while True:
        request = socket.recv()

        try:
            req_object = jd.decode(request)
        except:
            logger.warn("Request was not able to be decoded.")
        
        if req_object and REQUEST in req_object and DATA in req_object:
            response = dispatch(req_object['request'], req_object['data'])

        if response:
            socket.send(response)
        else:
            socket.send(ERROR)
        
        if jex_queue:
            workflow_provider.run_workflow(fetch_workflow())

        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Processes bioinformatic jobs.')

    parser.add_argument('port')
    parser.add_argument('--log')
    args = parser.parse_args()


    if args.log:
        log_level = getattr(logging, args.log.upper(), None)

        if not isinstance(log_level, int):
            raise ValueError('Invalid log level: %s' % log_level)
        logger.setLevel(log_level)
    
    listen_forever(args.port)
