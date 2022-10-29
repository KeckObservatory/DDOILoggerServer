from pprint import pprint
from pymongo import MongoClient
from datetime import datetime
import sys
from wonderwords import RandomSentence 
from random import choice 
import pdb
import time

if __name__ == '__main__':
    subsystem='MOSFIRE'
    config=None
    author="ttucker"
    progid="2022B"
    semid="1234"
    nRecords = 1000
    sendAck=True
    rs = RandomSentence()
    t1 = time.time()

    dbClient = MongoClient()
    logDB = dbClient["logs"]
    coll = logDB["logs"]

    for idx in range(nRecords):
        msg = rs.sentence()
        ident = 'create_logs.py'

        level = choice(["debug", "info", "warn", "error"])
        log = {
            'utc_sent': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%Z'),
            'utc_received': datetime.utcnow(),
            'hostname': f'{ident}',
            'level': level,
            'subsystem': subsystem,
            'author': author,
            'SEMID': semid,
            'PROGID': progid,
            'message': msg
        }
        coll.insert_one(log)
    t2 = time.time()
    print(t2-t1)



