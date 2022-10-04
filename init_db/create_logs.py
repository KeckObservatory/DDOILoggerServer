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
    logger = DDOILogger.DDOILogger(subsystem, config, author, progid, semid)
    rs = RandomSentence()
    t1 = time.time()
    for idx in range(nRecords):
        msg = rs.sentence()
        level = choice(["debug", "info", "warn", "error"])
        if level == "debug":
            logger.debug(msg)
        if level == "info":
            logger.info(msg)
        if level == "warn":
            logger.warn(msg)
        if level == "error":
            logger.error(msg)
    t2 = time.time()
    print(t2-t1)



