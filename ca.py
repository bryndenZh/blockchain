#! /usr/bin/env python3
import logging, traceback
import argparse
import yaml
from collections import Counter
import asyncio
from aiohttp import web
from logging.handlers import TimedRotatingFileHandler

from constants import MessageType

class CA():
    # _nodes: [{'host': 'xxx', 'port': xxx} , ...]
    def __init__(self, conf):
        self._nodes = conf['nodes']
        self._addr = conf['ca']
        self._log = logging.getLogger(__name__) 
        self._log.info('create ca at %s', str(self._addr))

    '''
    register a node to ca, type: application/json 
    request = {
        'host': xxx
        'port': xxx
    }
    '''
    async def register(self, request):
        node = await request.json()
        self._log.info("receive request to register %s", str(node))
        if node in self._nodes:
            self._log.info("already registered!")
            index = self._nodes.index(node)
        else:
            index = len(self._nodes)
            self._nodes.append(node)
        resp = {'index': index, 'nodes': self._nodes}
        self._log.info("return %s", str(resp))
        return web.json_response(resp)

def arg_parse():
    parser = argparse.ArgumentParser(description='CA Node')
    parser.add_argument('-i', '--index', type=int, help='node index')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')  
    args = parser.parse_args()
    return args

def logging_config(log_level=logging.DEBUG, log_file=None):
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return

    root_logger.setLevel(log_level)

    f = logging.Formatter("[%(asctime)s %(levelname)s] %(module)s->%(funcName)s: \t %(message)s")

    h = logging.StreamHandler()
    h.setFormatter(f)
    h.setLevel(log_level)
    root_logger.addHandler(h)

    if log_file:
        from logging.handlers import TimedRotatingFileHandler
        h = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7)
        h.setFormatter(f)
        h.setLevel(log_level)
        root_logger.addHandler(h)

def main():
    args = arg_parse()
    conf = yaml.safe_load(args.config)
    logging_config(log_level=logging.DEBUG ,log_file='~$ca.log')
    ca = CA(conf)
    app = web.Application()

    app.add_routes([
        web.post('/' + MessageType.REGISTER, ca.register)
    ])
    web.run_app(app, host=conf['ca']['host'], port=conf['ca']['port'])

if __name__ == "__main__":
    main()



