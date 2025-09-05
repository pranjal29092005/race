import os, sys, json, argparse
from race_server import RaceServer
import shutil, subprocess, uuid, time

parser = argparse.ArgumentParser(prog="get_race_status")
parser.add_argument(
    "--op", dest="op", choices=["spider", "pi", "geo-spider"], required=True
)
parser.add_argument("-interrupt-at", dest="interrupt_at", type=int, required=False)
parser.add_argument("--user", dest="user", type=str, required=False)
argv = parser.parse_args()

if not argv.user:
    argv.user = "raceclienttester@eigenrisk.com"

race_server = RaceServer(user=argv.user)
race_server.create_session()

if "spider" in argv.op:
    while 1:
        status = race_server.terminate_spider(True)
        reply = status.get("Reply")
        if not reply:
            continue
        print(reply)
        if reply == "OK":
            break
        # pc = reply.get('Percentage')
        # if pc is not None and float(pc) > 50.0:
        # race_server.terminate_spider('spider' in argv.op)
        # break
        # processed = reply.get('NumContractsProcessed')
        # print(total, processed)
        # if total and processed:
        # pc = processed / total * 100
        # print(f'{pc:5.2f}% done')
        #        else:
        #            race_server.terminate_spider()
        #            sys.exit(0)
        time.sleep(3)
else:
    status = race_server.get_pi_status()
