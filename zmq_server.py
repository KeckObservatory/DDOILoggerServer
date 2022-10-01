from datetime import datetime
import configparser
from pymongo import MongoClient
import zmq
import sys
import threading
import time
from ddoiloggerclient import DDOILogger
from wonderwords import RandomSentence
from random import randint, random, choice
import json
import os

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
        worker = self.context.socket(zmq.DEALER)
        worker.connect('inproc://backend')
        while True:
            ident, msg = worker.recv_multipart()
            self._handle_log(ident, msg)

    @staticmethod
    def _handle_log(ident, msg):

        content = json.loads(msg)
        db_client = get_mongodb()
        # Check to make sure that a valid subsystem and event type
        cursor = db_client.subsystems.find()
        subsystems = [s['identifier'] for s in cursor]
        cursor = db_client.levels.find()
        levels = [l['level'] for l in cursor]

        if content.get('subsystem', None) not in subsystems:
            return 'Invalid subsystem name', 400
        if content.get('level', None).lower() not in levels:
            return 'Invalid log level', 400

        log = {
            'utc_sent': content.get('utc_sent', None),
            'utc_recieved': datetime.utcnow(),
            'hostname': f'worker-{ident}',
            # 'ip_addr': str(request.remote_addr),
            'level': content.get('level', None),
            'subsystem': content.get('subsystem', None),
            'author': content.get('author', None),
            'SEMID': content.get('semid', None),
            'PROGID': content.get('progid', None),
            'message': content.get('message', None)
        }

        tprint('Worker received log from %s' % (ident))
        id = db_client.logs.insert_one(log)

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