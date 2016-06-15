# -*- coding: utf-8 -*-
import atexit
import json
import logging
from pprint import pformat
from time import sleep

import zmq
from yerba.core import (status_message, status_name, EventNotifier,
                        SCHEDULE_TASK, CANCEL_TASK, TASK_DONE)
from yerba.managers import (ServiceManager, WorkflowManager)
from yerba.routes import (route, dispatch)
from yerba.workflow import WorkflowError
from yerba.workqueue import WorkQueueService

logger = logging.getLogger('yerba')
access = logging.getLogger('access')
running = True
decoder = json.JSONDecoder()

MAX_WORK_QUEUES = 10

def listen_forever(config):
    notifier = EventNotifier()
    
    for i in range(MAX_WORK_QUEUES):
        try:
            wqconfig = config.items('workqueue' + (str(i) if i else '')) # workqueue, workqueue1, workqueue2...
        except:
            continue
        
        # Create WQ service
        wq = WorkQueueService(dict(wqconfig), notifier)
        ServiceManager.register(wq)
        notifier.register(CANCEL_TASK, wq.cancel)
        notifier.register(SCHEDULE_TASK, wq.schedule)
    
    # Create WQ manager
    WorkflowManager.connect(config.get('db', 'path'))
    WorkflowManager.set_notifier(notifier)
    WorkflowManager.cleanup()
    notifier.register(TASK_DONE, WorkflowManager.update)
    
    # Start WQ services
    ServiceManager.start()

    # Setup socket
    connection_string = "tcp://*:{}".format(config.get('yerba', 'port'))
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.set(zmq.LINGER, 0)
    socket.bind(connection_string)
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    atexit.register(shutdown)

    # Main loop
    while running:
        try:
            if socket in dict(poller.poll(timeout=10)):
                msg = None
                response = None

                try:
                    data = socket.recv_string()
                    msg = decoder.decode(data)
                    access.debug("ZMQ: Received \n%s", pformat(msg))
                except Exception:
                    logger.exception("ZMQ: The message was not parsed")

                if not msg:
                    logger.warn("The message was not received.")
                else:
                    try:
                        response = dispatch(msg)
                    except:
                        logger.exception("EXCEPTION")

                if not response:
                    logger.info("Invalid request")
                    response = {"status" : "Failed", "error": "Invalid response"}

                try:
                    message = json.dumps(response, encoding="utf-8", ensure_ascii=False)
                except Exception:
                    message = json.dumps({"status": "Failed", "error": "Invalid json"})
                    logger.exception("INVALID JSON RESPONSE:\n %s", pformat(response))

                try:
                    access.info("Sending Response")
                    socket.send_unicode(message, flags=zmq.NOBLOCK)
                except zmq.Again:
                    logger.exception("Failed to respond with response %s", response)
                finally:
                    access.info("Finished processing the response")
            else:
                try:
                    ServiceManager.update()
                except:
                    logger.exception("WORKQUEUE: Update error occurred")
        except:
            logger.exception("EXPERIENCED AN ERROR!")

        # Sleep for 5 milliseconds
        sleep(0.05)


@route("shutdown")
def shutdown():
    '''Shutdowns down the daemon'''
    running = False
    ServiceManager.stop()

#XXX: Add reporting information
@route("health")
def get_health(data):
    access.info("#### HEALTH CHECK #####")
    return  {"status" : "OK" }

@route("new")
def create_workflow(data):
    '''Returns the id of a new workflow object'''
    access.info("##### CREATING WORKFLOW #####")
    (workflow_id, status) = WorkflowManager.create()
    return { "status": status_name(status), "id": workflow_id }

@route("schedule")
def schedule_workflow(data):
    '''Returns the job id'''
    access.info("##### WORKFLOW SCHEDULING #####")
    (workflow_id, status, errors) = WorkflowManager.submit(data)

    return {
        "status" : status_name(status),
        "id": workflow_id,
        "errors": errors
    }

@route("restart")
def restart_workflow(data):
    '''Restart the job if it is running. Otherwise return NotFound'''
    access.info("##### WORKFLOW RESTART #####")
    try:
        identity = data['id']
        status = WorkflowManager.restart(identity)
        logger.info(status_message(identity, status))
        return {"status" : status_name(status)}
    except KeyError:
        return {"status" : 'NotFound'}

@route("cancel")
def cancel_workflow(data):
    '''Cancels the job if it is running.'''
    access.info("##### WORKFLOW CANCELLATION #####")
    try:
        identity = data['id']
        status = WorkflowManager.cancel(identity)
        logger.info(status_message(identity, status))
        return {"status" : status_name(status)}
    except KeyError:
        return {"status" : 'NotFound'}

@route("workflows")
def get_workflows(data):
    '''Return all matching workflows'''
    access.info("##### FETCHING WORKFLOWS #####")
    ids = None
    status = None

    if data:
        ids = data.get('ids', [])
        status = data.get('status', None)

    workflows = WorkflowManager.get_workflows(ids, status)
    result = []

    for (workflow_id, name, start, stop, status, priority) in workflows:
        status_message = status_name(status)
        result.append((workflow_id, name, start, stop, status_message))

    return { "workflows" : result }

@route("get_status")
def get_workflow_status(data):
    '''Gets the status of the workflow.'''
    access.info("##### WORKFLOW STATUS CHECK #####")
    try:
        identity = data['id']
        (status, jobs) = WorkflowManager.status(identity)
        logger.info(status_message(identity, status))
        return {"status" : status_name(status), "jobs" : jobs}
    except KeyError:
        return {"status" : 'NotFound', "jobs" : {}}
