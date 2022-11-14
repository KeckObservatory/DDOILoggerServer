import configparser
from flask import Flask, request, jsonify
from pymongo import MongoClient  
import pymongo
import os
from datetime import datetime
import sys
from urllib.parse import urlparse
from bson.json_util import dumps
import eventlet
from eventlet import wsgi
from zmq_server import get_mongodb
import pdb

###
# Setup
###

app = Flask(__name__)
app.config.from_object(__name__)
with app.app_context():
    try:
        get_mongodb()
    except Exception as e:
        print("Unable to access database")
        print(e.with_traceback())
        sys.exit(0)

###
# Routes
###


@app.route('/heartbeat', methods=["GET"])
def heartbeat():
    return "OK", 200


@app.route('/api/meta/valid_subsystems', methods=["GET"])
def get_subsystems():
    db_client = get_mongodb()
    subsystems = list(db_client.subsystems.find())
    if len(subsystems) > 0:
        res = jsonify([
            {"name": s['name'],
             "identifier": s['identifier']}
            for s in subsystems
        ])
        return res, 200
    else:
        return "No subsystem list found", 200


@app.route('/api/meta/valid_levels', methods=["GET"])
def get_levels():
    db_client = get_mongodb()
    levels = list(db_client.levels.find())
    if len(levels) > 0:
        res = jsonify([
            {"level": l['level']}
            for l in levels
        ])
        return res, 200
    else:
        return "No subsystem list found", 200


@app.route('/api/meta/add_subsystem', methods=["PUT"])
def add_subsystem():
    db_client = get_mongodb()
    db_client.subsystems.insert_one({
        "name": request.form['name'],
        "identifier": request.form['iden']
    })
    return 'Created', 201


@app.route('/api/meta/add_level', methods=["PUT"])
def add_level():
    db_client = get_mongodb()
    db_client.levels.insert_one({
        "level": request.form['level']
    })
    return 'Created', 201


@app.route('/api/log/new_log', methods=["PUT"])
def new_log():

    db_client = get_mongodb()

    content = request.form
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
        'utc_received': datetime.utcnow(),
        'hostname': str(urlparse(request.base_url).hostname),
        'ip_addr': str(request.remote_addr),
        'level': content.get('level', None),
        'subsystem': content.get('subsystem', None),
        'author': content.get('author', None),
        'SEMID': content.get('semid', None),
        'PROGID': content.get('progid', None),
        'message': content.get('message', None)
    }

    id = db_client.logs.insert_one(log)
    # print(f'Inserted {id} into DB')

    return "Log submitted", 201

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
        sort = [('utc_recieved', pymongo.DESCENDING) ]
    return find, sort


@app.route('/api/log/get_logs', methods=["GET"])
def get_logs():
    startDate = request.args.get('start_date', None, type=str)
    endDate = request.args.get('end_date', None, type=str)
    subsystem = request.args.get('subsystem', None, type=str)
    nLogs = request.args.get('n_logs', None, type=int)
    dateFormat = request.args.get('date_format', '%Y-%m-%d', type=str)

    find, sort = process_query(startDate, endDate, subsystem, nLogs, dateFormat)
    print('find', find)
    
    db_client = get_mongodb()
    cursor = db_client.logs.find(find) 
    if len(sort) > 0:
        cursor = cursor.sort(sort) 
    if nLogs:
        cursor = cursor.limit(nLogs)
    logs = list(cursor)
    if len(logs) > 0:
        res = dumps(logs)
        return res, 200
    else:
        return "No logs list found", 200

def get_default_config_loc():
    config_loc = os.path.abspath(os.path.dirname(__file__))
    config_loc = os.path.join(config_loc, './configs/server_cfg.ini')
    return config_loc

if __name__ == "__main__":

    config = get_default_config_loc()
    config_parser = configparser.ConfigParser()
    config_parser.read(config)
    config = config_parser['flaskserver']
    port = int(config.get('port'))
    wsgi.server(eventlet.listen(("127.0.0.1", port)), app)
