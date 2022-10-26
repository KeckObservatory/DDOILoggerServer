from datetime import datetime
import configparser
from pymongo import MongoClient
import pymongo
import zmq
import sys
import threading
import time
from wonderwords import RandomSentence
from random import randint, random, choice
import json
import os
import pdb


def process_query(startDate=None, endDate=None, subsystem=None, nLogs=None, dateFormat='%Y-%m-%d'):
    find = {}
    sort = []
    if not startDate is None and not endDate is None:
        sd = datetime.strptime(startDate, dateFormat)
        ed = datetime.strptime(endDate, dateFormat)
        find['utc_received'] = {'$lte': ed, '$gte': sd}
    elif startDate:
        sd = datetime.strptime(startDate, dateFormat)
        find['utc_received'] = {'$gte': sd}
    elif endDate:
        ed = datetime.strptime(endDate, dateFormat)
        find['utc_received'] = {'$lte': ed}
    if subsystem:
        find['subsystem'] = subsystem
    if nLogs:
        sort = [('utc_received', pymongo.DESCENDING) ]
    return find, sort

def get_mongodb():
    client = MongoClient(port = 27017)
    return client['logs'] 

def tprint(msg):
    """like print, but won't get newlines confused with multiple threads"""
    sys.stdout.write(msg + '\n')
    sys.stdout.flush()

def get_default_config_loc():
    config_loc = os.path.abspath(os.path.dirname(__file__))
    config_loc = os.path.join(config_loc, './configs/server_cfg.ini')

    return config_loc

class ServerTask(threading.Thread):
    """ServerTask"""

    def __init__(self, port, nworkers):
        threading.Thread.__init__(self)
        self.port = port
        self.nworkers = nworkers
        tprint(f'init server task. port: {port}, nworkers: {nworkers}')

    def run(self):
        context = zmq.Context()
        frontend = context.socket(zmq.ROUTER)
        frontend.bind(f'tcp://*:{self.port}')

        backend = context.socket(zmq.DEALER)
        backend.bind('inproc://backend')

        workers = []
        for idx in range(self.nworkers):
            worker = ServerWorker(context)
            worker.start()
            workers.append(worker)

        zmq.proxy(frontend, backend)

        frontend.close()
        backend.close()
        context.term()


class ServerWorker(threading.Thread):
    """ServerWorker"""

    def __init__(self, context):
        threading.Thread.__init__(self)
        self.context = context

    def run(self):
        """Main loop that continually monitors for messages.
        """
        worker = self.context.socket(zmq.DEALER)
        worker.connect('inproc://backend')
        while True:
            ident, msg = worker.recv_multipart()
            dmsg = json.loads(msg)
            msgType = dmsg.get('msg_type', '')
            msgBody = dmsg.get('body', None)
            print(dmsg)
            # route to proper function
            if msgType == 'request_logs' and msgBody is not None:
                resp = self._handle_request(msgBody)
            if msgType == 'log' and msgBody is not None:
                resp = self._handle_log(ident, msgBody)
            if msgType == 'request_metadata_options':
                resp = self._handle_metadata_options()
            if msgType == 'heartbeat':
                resp = self._handle_heartbeat_request()
            
            if not resp:
                resp = { 'resp': 400, 'msg': f"not able to process request {msgType}"} 
            # send response
            worker.send_multipart([ident, json.dumps(resp).encode()])
        worker.close()

    @staticmethod
    def _handle_heartbeat_request():
        """Used when worker recieves a heartbeat request. 
        Sends a simple response message to the requester.

        Returns:
            dict: message to be sent to requester
        """
        return {'msg': "OK", 'resp': 200}

    @staticmethod
    def _handle_metadata_options():
        """Gets subsystem and level arrays from database,
        and returns them in a dictionary.

        Returns:
            dict: message to be sent to requester
        """
        db_client = get_mongodb()
        subs = list(db_client.subsystems.find())
        subsystems = [{"name": s['name'], "identifier": s['identifier']} for s in subs]
        levs = list(db_client.levels.find())
        levels = [s['level'] for s in levs]

        if len(subsystems) <= 0 and len(levels) <=0:
            res = { 'resp': 200, 'msg': "No subsystem or levels found"} 
        if len(subsystems) <= 0:
            res = { 'resp': 200, 'msg': "No subsystem list found"} 
        elif len(levels) <= 0:
            res = { 'resp': 200, 'msg': "No subsystem list found"} 
        else:
            res = {'msg':{'subsystems': subsystems, 'levels': levels}, 'resp': 200} 
        return res

    @staticmethod
    def _handle_request(msg):
        """gets logs from database and returns a response to requester

        Args:
            ident (str): unique identifer of requester 
            msg (dict): message database query parameters 

        Returns:
            dict: message to be sent to requester 
        """

        startDate = msg.get('startDate', None)
        endDate = msg.get('endDate', None)
        nLogs = msg.get('nLogs', None)
        subsystem = msg.get('subsystem', None)
        dateFormat = msg.get('dateFormat', None)

        find, sort = process_query(startDate, endDate, subsystem, nLogs, dateFormat)

        try:
            db_client = get_mongodb()
            cursor = db_client.logs.find(find) 
            if len(sort) > 0:
                cursor = cursor.sort(sort) 
            if nLogs:
                cursor = cursor.limit(nLogs)
            logs = list(cursor)
            if len(logs) > 0:
                for log in logs:
                    log.pop('_id')
                    dt = log['utc_received']
                    log['utc_received'] = dt.strftime(dateFormat)
                res = {"msg": logs, "resp": 200} 
                return res 
            else:
                res = {"msg": "No logs list found", "resp": 200}
                return res
        except Exception as err:
            res = {"msg": f"error: {err}", "resp": 400}
            return res

    @staticmethod
    def _handle_log(ident, msg):
        """Adds msg to database and returns a response to requester

        Args:
            ident (str): unique identifer of requester 
            msg (dict): message that is to be added to the database 

        Returns:
            dict: message to be sent to requester 
        """

        log = {
            'utc_sent': msg.get('utc_sent', None),
            'utc_received': datetime.utcnow(),
            'hostname': f'{ident}',
            'level': msg.get('level', None),
            'subsystem': msg.get('subsystem', None),
            'author': msg.get('author', None),
            'SEMID': msg.get('semid', None),
            'PROGID': msg.get('progid', None),
            'message': msg.get('message', None)
        }

        db_client = get_mongodb()
        # Check to make sure that a valid subsystem and event type
        cursor = db_client.subsystems.find()
        subsystems = [s['identifier'] for s in cursor]
        cursor = db_client.levels.find()
        levels = [l['level'] for l in cursor]

        if msg.get('subsystem', None) not in subsystems:
            return {'msg': 'Invalid subsystem name', 'log': log, 'resp': 400}
        if msg.get('level', None).lower() not in levels:
            return {'msg': 'Invalid subsystem name', 'log': log, 'resp': 400}

        # tprint('Worker received log from %s' % (ident))
        try: 
            id = db_client.logs.insert_one(log)
            resp = {'resp': 200, 'msg': f'log submitted to database. id: {id.inserted_id}'}
        except Exception as err:
            resp = {'resp': 400, 'log': log, 'msg': f'log not submitted to database. err: {err}'}
        return resp
            

if __name__ == "__main__":

    config = get_default_config_loc()
    config_parser = configparser.ConfigParser()
    config_parser.read(config)

    config = config_parser['zeromqserver']
    port = config.get('port', '5570')
    nworkers = int(config.get('nworkers', 1))
    server = ServerTask(port, nworkers)
    server.start()
    server.join()