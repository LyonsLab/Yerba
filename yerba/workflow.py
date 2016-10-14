# -*- coding: utf-8 -*-
from itertools import groupby
import logging
import os

from yerba import core
from yerba import utils
from yerba.task import Task

logger = logging.getLogger('yerba.workflow')

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


@utils.log_on_exception(OSError, "The task could not be written to the log.",
                         logger=logger)
@utils.log_on_exception(IOError, "The task could not be written to the log.",
                         logger=logger)
def log_task_info(log_file, task):
    '''Log the results of a task'''
    outputs = []
    msg = (
        "task: {cmd}\n"
        "Submitted at: {started}\n"
        "Completed at: {ended}\n"
        "Execution time: {elapsed} sec\n"
        "Assigned to task: {taskid}\n"
        "Return status: {returned}\n"
        "Expected outputs: {outputs}\n"
        "Command Output:\n"
        "{output}")

    for item in task.outputs:
        if isinstance(item, list) and item[1]:
            outputs.append(item[0])
        else:
            outputs.append(item)

    task.info['outputs'] = ', '.join(outputs)
    description = '{0}\n'.format(task.description)
    body = msg.format(**task.info)

    with open(log_file, 'a') as log_handle:
        log_handle.write('#' * 25 + '\n')
        log_handle.write(description)
        log_handle.write(body)
        log_handle.write('#' * 25 + '\n\n')

@utils.log_on_exception(OSError, "The task could not be written to the log.",
                         logger=logger)
@utils.log_on_exception(IOError, "The task could not be written to the log.",
                         logger=logger)
def log_skipped_task(log_file, task):
    '''Log a task that was skipped'''
    with open(log_file, 'a') as log_handle:
        log_handle.write('#' * 25 + '\n')
        log_handle.write('{0}\n'.format(task.description))
        log_handle.write("task: %s\n" % str(task))
        log_handle.write("Skipped: The analysis was previously generated.\n")
        log_handle.write('#' * 25 + '\n\n')

@utils.log_on_exception(OSError, "The task could not be written to the log.",
                         logger=logger)
@utils.log_on_exception(IOError, "The task could not be written to the log.",
                         logger=logger)
def log_not_run_task(log_file, task):
    '''Log a task that could not be run'''

    with open(log_file, 'a') as log_handle:
        log_handle.write('#' * 25 + '\n')
        log_handle.write('{0}\n'.format(task.description))
        log_handle.write("task: %s\n" % str(task))
        log_handle.write("The task was not run.\n")
        log_handle.write('#' * 25 + '\n\n')

#FIXME: states for tasks should be decoupled from tasks
#TODO: Add proper dependency management to tasks
class Workflow(object):
    def __init__(self, name, tasks, log=None, priority=0):
        self.name = name
        self.log = log
        self.priority = priority
        self.tasks = tuple(tasks)
        self.available = tasks
        self.running = []
        self.completed = []
        self.status = core.Status.Initialized

    def update_status(self, task, info):
        '''Updates the status of the workflow'''
        #: Assign the info object to the task
        task.info = info

        #: Remove the task from the running list
        self.running.remove(task)

        #FIXME: add workflow change events
        #: Update the workflow log
        if self.log:
            log_task_info(self.log, task)

        #: Check that task returned successfully
        if info['returned'] != 0 or not task.completed():
            task.status = FAILED
            self._failed()
            self.completed.append(task)
            self.status = core.Status.Failed
            return self.status

        #: Update the status to completed
        task.status = COMPLETED

        #: Check if the workflow is already in a finished state
        if self.status in core.DONE_STATUS:
            return self.status

        #: Check if the workflow finished
        if self._finished():
            self.status = core.Status.Completed
            return self.status

        #: Check that the workflow can proceed from this point
        if self._can_proceed():
            self.status = core.Status.Running
            return self.status
        else:
            self._failed()
            self.status = core.Status.Failed
            return self.status

    def next(self):
        '''Return the next set of available tasks'''
        available = []
        skipped = []

        #: Check if the workflow is already in a finished state
        if self.status in core.DONE_STATUS:
            return available

        for task in self.available:
            if task.outputs and task.completed():
                skipped.append(task)
                continue

            if task.ready() and task.status in READY_STATES:
                self.available.remove(task)
                self.running.append(task)
                available.append(task)
                task.status = RUNNING

        for task in skipped:
            self._skip(task)

        #: Check if any tasks are busy
        if available or self.running:
            self.status = core.Status.Running
        elif not self.available:
            #: Check if all tasks have been skipped
            self.status = core.Status.Completed
        else:
            self._failed()
            self.status = core.Status.Failed

        return available

    def cancel(self):
        ''' Sets the state of the workflow as cancelled'''
        self.status = core.Status.Cancelled

        for task in self.available + self.running:
            if task in RUNNING_STATES:
                task.status = CANCELLED

    def stop(self):
        ''' Sets the state of the workflow as stopped'''
        self.status = core.Status.Stopped

        for task in self.available + self.running:
            if task in RUNNING_STATES:
                task.status = STOPPED

    def state(self):
        """Returns the state of the workflow"""
        return [task.state for task in self.tasks]

    def _finished(self):
        """Returns True when all tasks have been finished"""
        return not self.available and not self.running

    def _can_proceed(self):
        """
        Returns whether the workflow can continue.
        """
        #: Get the number of running tasks
        total_running = len(self.running)

        #: Check if any tasks are still waiting
        waiting = [task for task in self.available if task.status in READY_STATES]

        #: Get the number of ready tasks
        total_ready = len([task for task in waiting if task.ready()]) > 0

        #: Proceed if a task is running or a task is ready
        return total_ready or total_running

    def _failed(self):
        '''Sets a task into the failed state'''
        for task in self.available:
            task.status = FAILED
            #FIXME: add workflow change events
            #: Update the workflow log
            if self.log:
                log_not_run_task(self.log, task)

    def _skip(self, task):
        '''Sets a task into a skipped state'''
        task.status = SKIPPED
        self.available.remove(task)
        self.completed.append(task)

        #: Update the workflow log
        if self.log:
            log_skipped_task(self.log, task)

    def status_message(self):
        prefix = "WORKFLOW{0}: " % self.name
        states = sorted(task.state for task in self.available + self.completed)

        #: summarize the number of tasks in each state
        fields = [",".join([state, len(tasks)]) for state,tasks in groupby(states)]

        #: summary of the workflows status
        summary = " ".join(fields)

        return prefix + summary

    @classmethod
    def from_object(cls, workflow_object):
        '''Generates a workflow from a python object.'''
        logger.info("######### Generate Workflow  ##########")
        task_objects = workflow_object.get('tasks', [])

        if not task_objects:
            raise WorkflowError("The workflow does not contain any tasks.")

        name = workflow_object.get('name', 'unnamed')
        level = workflow_object.get('priority', 0)
        logfile = workflow_object.get('logfile')

        #: Create the directory to the log file
        with utils.ignored(OSError, AttributeError):
            os.makedirs(os.path.dirname(logfile))

        # Verify tasks and save errors
        errors = []
        for (index, task_object) in enumerate(task_objects):
            (valid, reason) = validate_task(task_object)

            if not valid:
                errors.append((index, reason))

        if errors:
            raise WorkflowError("%s tasks where not valid." % len(errors), errors)

        tasks = [Task.from_object(task_object) for task_object in task_objects]
        workflow = cls(name, tasks, log=logfile, priority=level)
        logger.info("WORKFLOW %s has been generated.", name)
        return workflow

def validate_task(task_object):
    """
    Returns if the task is valid or gives a reason why invalid.
    """

    cmd = task_object.get('cmd', None)
    args = task_object.get('args', [])
    inputs = task_object.get('inputs', []) or []
    outputs = task_object.get('outputs', []) or []

    if not cmd:
        return (False, "The command name was not specified")

    if not isinstance(args, list):
        return (False, "The task expected a list of arguments")

    if not isinstance(inputs, list):
        return (False, "The task expected a list of inputs")

    if not isinstance(outputs, list):
        return (False, "The task expected a list of outputs")

    if any(item is None for item in inputs):
        return (False, "An input was invalid")

    if any(fp is None for fp in outputs):
        return (False, "An output was invalid")

    return (True, "The task has been validated")

class WorkflowError(Exception):
    """Error reported when workflow fails to be created"""
    def __init__(self, message, errors=None):
        Exception.__init__(self, message)
        self.errors = errors
