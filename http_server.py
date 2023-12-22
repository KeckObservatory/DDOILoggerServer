import yaml
import pdb
import argparse
from flask import Flask, request 
from datetime import datetime
from urllib.parse import urlparse
from bson.json_util import dumps
import eventlet
from eventlet import wsgi
from zmq_server import get_mongodb, process_query, get_default_config_loc

app = Flask(__name__)
app.config.from_object(__name__)

@app.route('/heartbeat', methods=["GET"])
def heartbeat():
    return "OK", 200


@app.route('/api/log/new_log', methods=["PUT"])
def new_log():
    content = request.form
    database = content.get('database', 'DDOI')
    log = {
        'utc_sent': content.get('utc_sent', None),
        'utc_received': datetime.utcnow(),
        'hostname': str(urlparse(request.base_url).hostname),
        'message': content.get('message', None),
    }
    dbconfig = config[f'{database}_DATA_BASE']
    db_name = dbconfig.get('DB_NAME')
    db_client = get_mongodb(db_name)
    log_coll_name = dbconfig.get('LOG_COLL_NAME')
    log_schema = dbconfig.get('LOG_SCHEMA').replace(' ', '').split(',')
    for key in log_schema:
        log[key] = content.get(key, None)
    id = db_client[log_coll_name].insert_one(log)
    return "Log submitted", 201


@app.route('/api/log/get_logs', methods=["GET"])
def get_logs():
    startDate = request.args.get('start_date', None, type=str)
    endDate = request.args.get('end_date', None, type=str)
    nLogs = request.args.get('n_logs', None, type=int)
    dateFormat = request.args.get('date_format', '%Y-%m-%d', type=str)
    
    dbconfig = config[f'{database}_DATA_BASE']
    db_name = dbconfig.get('DB_NAME')
    log_coll_name = dbconfig.get('LOG_COLL_NAME')
    log_schema = dbconfig.get('LOG_SCHEMA').replace(' ', '').split(',')

    query_params = { key: request.args.get(key, None) for key in log_schema }
    database = request.args.get('database', 'DDOI', type=str)

    find, sort = process_query(startDate, endDate, nLogs, dateFormat, **query_params)
    
    db_client = get_mongodb(db_name)

    cursor = db_client[log_coll_name].find(find) 
    if len(sort) > 0:
        cursor = cursor.sort(sort) 
    if nLogs:
        cursor = cursor.limit(nLogs)
    logs = [x for x in cursor]
    if len(logs) > 0:
        res = dumps(logs)
        return res, 200
    else:
        return "No logs list found", 200


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create zmq server")
    parser.add_argument('--configPath', type=str, required=False, default=get_default_config_loc(),
                         help="subsystem specific logs")
    args = parser.parse_args()
    with open(args.configPath, 'r') as f:
        config = yaml.safe_load(f)
    flaskconfig = config['FLASK_SERVER']
    url = flaskconfig.get('URL')
    port = int(flaskconfig.get('PORT', None))
    wsgi.server(eventlet.listen((url, port)), app)
