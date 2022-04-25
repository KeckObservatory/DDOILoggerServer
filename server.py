from re import L
from flask import Flask, request, jsonify
from pymongo import MongoClient

import json
from datetime import datetime
import sys
from urllib.parse import urlparse
from bson.json_util import dumps

###
# Setup
###

def get_mongodb():
    client = MongoClient(port = 27017)
    return client

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

@app.route('/api/meta/valid_subsystems', methods=["GET"])
def get_subsystems():
    db_client = get_mongodb()
    subsystems = list(db_client.presets.subsystems.find())
    if len(subsystems) > 0:
        res = jsonify([
            {"name" : s['name'],
            "identifier" : s['identifier']}
            for s in subsystems
        ])
        return res, 200
    else:
        return "No subsystem list found", 200

@app.route('/api/meta/valid_levels', methods=["GET"])
def get_levels():
    db_client = get_mongodb()
    levels = list(db_client.presets.levels.find())
    if len(levels) > 0:
        res = jsonify([
            {"level" : l['level']}
            for l in levels
        ])
        return res, 200
    else:
        return "No subsystem list found", 200

@app.route('/api/meta/add_subsystem', methods=["PUT"])
def add_subsystem():
    db_client = get_mongodb()
    db_client.presets.subsystems.insert({
        "name" : request.form['name'],
        "identifier" : request.form['iden']
    })
    return 'Created', 201

@app.route('/api/meta/add_level', methods=["PUT"])
def add_level():
    db_client = get_mongodb()
    db_client.presets.levels.insert({
        "level" : request.form['level']
    })
    return 'Created', 201

@app.route('/api/log/new_log', methods=["PUT"])
def new_log():

    db_client = get_mongodb()

    content = request.form
    # Check to make sure that a valid subsystem and event type
    subsystems = [s['identifier'] for s in list(db_client.presets.subsystems.find())]
    levels = [l['level'] for l in list(db_client.presets.levels.find())]

    if content['subsystem'] not in subsystems:
        return 'Invalid subsystem name', 400
    if content['level'] not in levels:
        return 'Invalid log level', 400
    
    try:
        log = {
            'id' : content['event_id'],
            'utc_sent' : content['utc'],
            'utc_recieved' : datetime.utcnow(),
            'hostname' : str(urlparse(request.base_url).hostname),
            'ip_addr' : str(request.remote_addr),
            'level' : content['level'],
            'subsystem' : content['subsystem'],
            'author' : content['author'],
            'SEMID' : content['semid'] if content['semid'] else "NONE",
            'PROGID' : content['progid'] if content['progid'] else "NONE",
            'message' : content['message']
        }
    except KeyError:
        return 'Improperly formatted log', 400

    id = db_client.logger.log.insert(log)
    print(f'Inserted {id} into DB')

    return "Log submitted", 201

@app.route('/api/log/get_logs', methods=["GET"])
def get_logs():
    db_client = get_mongodb()
    logs = list(db_client.logger.log.find())
    if len(logs) > 0:
        res = dumps(logs)
        return res, 200
    else:
        return "No logs list found", 200

if __name__ == "__main__":
    app.run(debug=True)