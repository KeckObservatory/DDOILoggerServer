from datetime import datetime
import configparser
import argparse
from pymongo import MongoClient, DESCENDING
import zmq
import sys
import threading
import json
import os
import pdb


def process_query(startDate=None, endDate=None, nLogs=None, dateFormat='%Y-%m-%d', **query_params):
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
    for key, val in query_params.items():
        if val:
            find[key] = val
    if nLogs:
        sort = [('utc_received', DESCENDING)]
    return find, sort


def get_mongodb(db_name):
    client = MongoClient(port=27017)
    return client[db_name]


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
        for _ in range(self.nworkers):
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


    def process_request(self, ident, msg):
        """processes request and returns a dictionary

        Args:
            ident (str): unique identity of requester
            msg (bstr): message recieved by requester

        Returns:
            dict: comprises of {'msg': string | dict, 'resp': 200 | 400 } 
        """
        try:
            dmsg = json.loads(msg)
            msgType = dmsg.get('msg_type', '')
            msgBody = dmsg.get('body', None)
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
                resp = {'resp': 400,
                        'msg': f"not able to process request {msgType}"}
        except Exception as err:
            resp = {'resp': 400, 'msg': f"server encountered error: {err}"}
        return resp

    def run(self):
        """Main loop that continually monitors for messages.
        """
        worker = self.context.socket(zmq.DEALER)
        worker.connect('inproc://backend')
        while True:
            ident, msg = worker.recv_multipart()
            resp = self.process_request(ident, msg)
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
    def _handle_request(msg):
        """gets logs from database and returns a response to requester

        Args:
            ident (str): unique identifer of requester 
            msg (dict): message database query parameters 

        Returns:
            dict: message to be sent to requester 
        """

        nLogs = msg.get('nLogs')
        DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%Z'
        dateFormat = msg.get('dateFormat', DATE_FORMAT)

        pqargs = {
            'startDate': msg.get('startDate', None),
            'endDate': msg.get('endDate', None),
            'nLogs': nLogs,
            'dateFormat': dateFormat
        }

        for key in log_schema:
            pqargs[key] = msg.get(key, None)
            

        find, sort = process_query(**pqargs)
        try:
            db_client = get_mongodb(db_name)
            cursor = db_client[log_coll_name].find(find)
            if len(sort) > 0:
                cursor = cursor.sort(sort)
            if nLogs:
                cursor = cursor.limit(nLogs)
            logs = [x for x in cursor]
            if len(logs) > 0:
                for log in logs:
                    log.pop('_id')
                    dt = log['utc_received']
                    log['utc_received'] = dt.strftime(dateFormat)
                    log['utc_sent'] = dt.strftime(dateFormat)
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
            'message': msg.get('message', None)
        }

        for key in log_schema:
            log[key] = msg.get(key, None)
            
        db_client = get_mongodb(db_name)

        try:
            id = db_client[log_coll_name].insert_one(log)
            resp = {'resp': 200,
                    'msg': f'log submitted to database. id: {id.inserted_id}'}
        except Exception as err:
            resp = {'resp': 400, 'log': log,
                    'msg': f'log not submitted to database. err: {err}'}
        return resp


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create zmq server")
    parser.add_argument('--configPath', type=str, required=False, default=get_default_config_loc(),
                        help="subsystem specific logs")
    args = parser.parse_args()

    config_parser = configparser.ConfigParser()
    config_parser.read(args.configPath)
    zmqconfig = config_parser['ZMQ_SERVER']
    dbconfig = config_parser['DATA_BASE']
    url = zmqconfig.get('URL')
    port = int(zmqconfig.get('PORT'))
    log_schema = dbconfig.get('LOG_SCHEMA').replace(' ', '').split(',')
    log_coll_name = dbconfig.get('LOG_COLL_NAME')
    db_name = dbconfig.get('DB_NAME')
    nworkers = int(zmqconfig.get('N_WORKERS', 1))

    server = ServerTask(port, nworkers)
    server.start()
    server.join()
