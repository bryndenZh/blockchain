#! /usr/bin/env python3
import logging, traceback
import argparse
import yaml
from collections import Counter
import asyncio
from aiohttp import web

from constants import MessageType

class CA():
    # _nodes: [{'host': 'xxx', 'port': xxx} , ...]
    def __init__(self, conf):
        self._nodes = conf['nodes']
        self.addr = conf['ca']

    '''
    register a node to ca, type: application/json 
    request = {
        'host': xxx
        'port': xxx
    }
    '''
    async def register(self, request):
        node = await request.json()
        logging.info("receive request to register %s", str(node))
        self._nodes.append(node)
        resp = {'index': len(self._nodes)}
        return web.json_response(resp)

def arg_parse():
    parser = argparse.ArgumentParser(description='CA Node')
    parser.add_argument('-i', '--index', type=int, help='node index')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')  
    args = parser.parse_args()
    return args


def main():
    logging.basicConfig(level=logging.DEBUG ,filename='~$ca.log', format="[%(asctime)s %(levelname)s] %(module)s->%(funcName)s: \t %(message)s")
    args = arg_parse()
    conf = yaml.safe_load(args.config)
    ca = CA(conf)
    app = web.Application()

    app.add_routes([
        web.post('/' + MessageType.REGISTER, ca.register)
    ])
    web.run_app(app, host=conf['ca']['host'], port=conf['ca']['port'])

if __name__ == "__main__":
    main()



