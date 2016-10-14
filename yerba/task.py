# -*- coding: utf-8 -*-
import logging
import os

from yerba import utils

logger = logging.getLogger('yerba.task')

WAITING   = 'waiting'
SCHEDULED = 'scheduled'
RUNNING   = 'running'
COMPLETED = 'completed'
FAILED    = 'failed'
CANCELLED = 'cancelled'
STOPPED   = 'stopped'
SKIPPED   = 'skipped'

READY_STATES    = frozenset([WAITING, SCHEDULED])
RUNNING_STATES  = frozenset([WAITING, SCHEDULED, RUNNING])
FINISHED_STATES = frozenset([STOPPED, CANCELLED, FAILED, COMPLETED, SKIPPED])

def _format_args(args):
    """Returns given a list of args returns an argument string"""
    argstring = ""

    for (arg, value, shorten) in args:
        val = str(value)

        if shorten == 1 and os.path.isabs(val):
            val = os.path.basename(val)

        argstring = ("%s %s %s" % (argstring, arg, val))

    return argstring

class Task(object):
    def __init__(self, cmd, arguments, description='', priority=0):
        self.cmd = cmd
        self.args = arguments
        self.inputs = []
        self.outputs = []
        self._status = SCHEDULED
        self.description = description
        self._info = {}
        self._errors = []
        self.attempts = 1
        self._options = {
            "allow-zero-length" : True,
            "retries" : 0
        }
        self.priority = priority; # mdb added 6/20/16 for jex-distribution
        if self.priority == None:
            self.priority = 0;

    @classmethod
    def from_object(cls, task_object):
        """
        Returns a task generated from a python object
        """
        (cmd, priority, args) = (task_object['cmd'], task_object.get('priority'), task_object.get('args', []))

        arg_string = _format_args(args)

        # Set the task_object description
        desc = task_object.get('description', '')
        new_task = cls(cmd, arg_string, description=desc, priority=priority)
        logger.debug("Creating task '%s'",  new_task.description)

        # Set the task_object options
        options = task_object.get('options', {})
        logger.info("Additional task options being set: %s", options)
        new_task.options = filter_options(options)

        # Add inputs
        inputs = task_object.get('inputs', []) or []
        new_task.inputs.extend(sorted(inputs))

        # Add outputs
        outputs = task_object.get('outputs', []) or []
        new_task.outputs.extend(sorted(outputs))

        if 'overwrite' in task_object and int(task_object['overwrite']):
            logger.debug(("The task will overwrite previous results:\n%s"), new_task)
            new_task.clear()

        return new_task

    @property
    def options(self):
        return self._options

    @options.setter
    def options(self, options):
        """
        Updates the options to be used by the task
        """
        self._options = utils.ChainMap(options, self._options)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        logger.info('Task: the status has been changed to %s', value)
        self._status = value

    @property
    def info(self):
        return self._info

    @info.setter
    def info(self, info):
        logger.info("Task (status: %s): The info field has been updated", self.status)
        self._info = info

    @property
    def state(self):
        #FIXME add support for errors

        status = [
            ['status',      self.status],
            ['description', self.description],
            ['cmd',         self.cmd + self.args],  # mdb added 10/13/16
            ['inputs',      self.inputs],           # mdb added 10/13/16
            ['outputs',     self.outputs]           # mdb added 10/13/16
        ]

        status.extend(self.info.items())

        return dict(status)

    def clear(self):
        for output in self.outputs:
            with utils.ignored(OSError):
                os.remove(output)

    def running(self):
        return self._status == 'running'

    def completed(self):
        '''Returns whether or not the task was completed.'''

        for fp in self.outputs:
            if isinstance(fp, list) and fp[1]:
                val = os.path.abspath(str(fp[0]))

                if not os.path.isdir(val):
                    return False

            elif self.options["allow-zero-length"]:
                path = os.path.abspath(str(fp))

                if not os.path.isfile(path):
                    return False
            else:
                path = os.path.abspath(str(fp))
                if not os.path.isfile(path) or utils.is_empty(path):
                    return False

        return True

    def ready(self):
        '''Returns that the task has its input files and is ready.'''
        for fp in self.inputs:
            if isinstance(fp, list) and fp[1]:
                val = os.path.abspath(str(fp[0]))

                if not os.path.isdir(val):
                    return False
            elif self.options["allow-zero-length"]:
                path = os.path.abspath(str(fp))

                if not os.path.isfile(path):
                    return False
            else:
                path = os.path.abspath(str(fp))

                if not os.path.isfile(path) or utils.is_empty(path):
                    return False

        return True

    def restart(self):
        self.attempts = self.attempts + 1

    def failed(self):
        return self.attempts > self.options['retries']

    def __eq__(self, other):
        return (sorted(other.inputs) == sorted(self.inputs) and
                sorted(other.outputs) == sorted(self.outputs) and
                str(other) == str(self))

    def __repr__(self):
        return ' '.join([self.cmd, self.args])

    def __str__(self):
        return repr(self)

def filter_options(options):
    """
    Returns the set of filtered options that are specified
    """
    return {key : value for (key, value) in options.iteritems()
                if value is not None}