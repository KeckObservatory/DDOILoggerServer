from pprint import pprint
from datetime import datetime
import sys
from ddoiloggerclient import DDOILogger 
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
    logger = DDOILogger.DDOILogger(subsystem, config, author, progid, semid)
    rs = RandomSentence()
    t1 = time.time()
    pdb.set_trace()
    failedLogs = logger.read_failed_logs
    for idx in range(nRecords):
        msg = rs.sentence()
        level = choice(["debug", "info", "warn", "error"])
        if level == "debug":
            resp = logger.debug(msg, sendAck=sendAck)
        if level == "info":
            resp = logger.info(msg, sendAck=sendAck)
        if level == "warn":
            resp = logger.warn(msg, sendAck=sendAck)
        if level == "error":
            resp = logger.error(msg, sendAck=sendAck)
        if sendAck:
            logger.handle_response(resp, msg)
    t2 = time.time()
    print(t2-t1)



