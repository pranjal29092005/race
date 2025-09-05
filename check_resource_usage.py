import os, sys
from race_server import RaceServer as rs

server = rs()
server.check_resource_usage()
