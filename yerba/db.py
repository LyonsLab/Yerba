from json import JSONEncoder
from sqlite3 import connect, IntegrityError
from time import time

from yerba.core import Status

CREATE_TABLE_QUERY = '''
    CREATE TABLE IF NOT EXISTS workflows
    (id INTEGER PRIMARY KEY AUTOINCREMENT,
     workflow BLOB,
     submitted TEXT,
     completed TEXT,
     status INTEGER)
'''

START_INDEX_QUERY = '''
    UPDATE SQLITE_SEQUENCE
    SET seq=?
    WHERE name='workflows'
'''

FIND_WORKFLOW_QUERY = '''
    SELECT * FROM workflows
    WHERE workflow=?
'''

GET_STATUS_QUERY = '''
    SELECT status FROM workflows
    WHERE id=?
'''

INSERT_WORKFLOW_QUERY = '''
    INSERT INTO workflows(workflow, submitted, completed, status)
    VALUES (?, ?, ?, ?)
'''

UPDATE_FIELD_QUERY = '''
    UPDATE workflows
    SET {field}=? WHERE id=?
'''

GET_WORKFLOWS_QUERY = '''
    SELECT id, submitted, completed, status
    FROM workflows
'''

GET_WORKFLOWS_IN_QUERY = '''
    SELECT id, submitted, completed, status
    FROM workflows WHERE id IN ({ids})
'''

encoder = JSONEncoder()

class Database(object):
    """
    A minimal interface that abstract the sqlite api
    """

    def __init__(self):
        self.handle = None

    def connect(self, filename):
        """
        Returns a connection to the database
        """
        self.handle = connect(filename)

    def execute(self, query, params=()):
        """
        Executes a query on the database
        """
        try:
            with self.handle:
                cursor = self.handle.execute(query, params)
            return cursor
        except IntegrityError:
            pass

    def close(self):
        """
        Closes the connect to the database
        """
        self.handle.close()

def setup(filename, start_index=0):
    """
    Creates the workflow table and reset the starting index
    """
    database = connect(filename)
    database.execute(CREATE_TABLE_QUERY)
    database.execute(START_INDEX_QUERY, (start_index,))
    database.close()

def get_status(database, workflow_id):
    """
    Returns the status of the workflow
    """

    cursor = database.execute(GET_STATUS_QUERY, (workflow_id,))
    row = cursor.fetchone()

    if row:
        return row[0]
    else:
        return Status.NotFound

def find_workflow(database, workflow):
    """
    Finds the workflow and returns its id
    """
    workflow_json = encoder.encode(workflow)
    cursor = database.execute(FIND_WORKFLOW_QUERY, (workflow_json,))
    return cursor.fetchone()

def add_workflow(database, workflow_object=None, status=Status.Initialized):
    """
    Adds the workflow and returns its id
    """

    if workflow_object:
        jobs = workflow_object['jobs']
        params = (encoder.encode(jobs), time(), None, status)
    else:
        params = (None, time(), None, status)

    cursor = database.execute(INSERT_WORKFLOW_QUERY, params)
    return str(cursor.lastrowid)

def update_status(database, workflow_id, status, completed=False):
    """
    Updates the status of the workflow
    """
    query = UPDATE_FIELD_QUERY.format(field="status")
    params = (status, workflow_id)
    database.execute(query, params)

    if completed:
        query = UPDATE_FIELD_QUERY.format(field="completed")
        params = (time(), workflow_id)
        database.execute(query, params)

def get_workflows(database, ids=None):
    """
    Returns a subset of workflows

    If ids is specified the workflows will be limited to the subset of
    of workflows with matching ids.
    """
    if ids:
        id_string = ",".join(set(str(workflow_id) for workflow_id in ids))
        cursor = database.execute(GET_WORKFLOWS_IN_QUERY.format(ids=id_string))
    else:
        cursor = database.execute(GET_WORKFLOWS_QUERY)

    return cursor.fetchall()
