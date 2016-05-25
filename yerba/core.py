# -*- coding: utf-8 -*-
from collections import namedtuple, defaultdict

_status_types = [
    "Initialized",
    "Scheduled",
    "Running",
    "Completed",
    "Cancelled",
    "Stopped",
    "Failed",
    "NotFound",
    "Error"
]

def status_name(code):
    return _status_types[code]

indices = range(len(_status_types))
Status = namedtuple('Status', ' '.join(_status_types))._make(indices)

def status_code(name):
    return getattr(Status, name.capitalize())

DONE_STATUS = frozenset([Status.Failed, Status.Completed, Status.Cancelled, Status.Stopped])

_status_messages = {
    Status.Initialized: "The workflow {0} has been initalized",
    Status.Scheduled: "The workflow {0} has been scheduled.",
    Status.Running: "The workflow {0} is running.",
    Status.Completed: "The workflow {0} was completed.",
    Status.Cancelled: "The workflow {0} has been cancelled.",
    Status.Stopped: "The workflow {0} has been stopped.",
    Status.Failed: "The workflow {0} failed.",
    Status.NotFound: "The workflow {0} was not found.",
    Status.Error: "The workflow {0} has errors."
}

def status_message(name, code):
    return _status_messages[code].format(name)

SCHEDULE_TASK = 'schedule'
CANCEL_TASK = 'cancel'
TASK_DONE = 'done'

class EventNotifier(object):
    def __init__(self):
        self.events = defaultdict(list)

    def notify(self, event, *args, **kw):
        '''
        Notify all registered receivers for the event
        '''
        for callback in self.events[event]:
            callback(*args, **kw)

    def register(self, event, receiver):
        '''
        Register the receiver to be notified for the event
        '''
        self.events[event].append(receiver)

    def unregister(self, event, receiver):
        '''
        Unregister the receiver from event
        '''
        self.events[event].remove(receiver)
