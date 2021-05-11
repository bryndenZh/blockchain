#! /usr/bin/env python3
import logging, traceback
from spbft_handler import SPBFTHandler
import argparse
import yaml
from collections import Counter
import asyncio
from aiohttp import web

from pbft_handler import PBFTHandler
from constants import MessageType
from dynamic_pbft_handler import DynamicPBFTHandler

VIEW_SET_INTERVAL = 10


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

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def arg_parse():
    # parse command line options
    parser = argparse.ArgumentParser(description='PBFT Node')
    parser.add_argument('-i', '--index', type=int, help='node index')
    parser.add_argument('-c', '--config', default='pbft.yaml', type=argparse.FileType('r'), help='use configuration [%(default)s]')  
    parser.add_argument('--host', help='custom host address')
    parser.add_argument('--port', help='custom port')
    args = parser.parse_args()
    return args

def conf_parse(conf_file) -> dict:
    conf = yaml.safe_load(conf_file)
    return conf


    

def main():
    args = arg_parse()
    conf = conf_parse(args.config)

    # if set --host, use it instead of config
    if args.host and args.port:
        host = args.host
        port = args.port
        print(host, port)
        logging_config(log_level=logging.DEBUG ,log_file='~$node_' + host + ':' + port +'.log')
        log = logging.getLogger()
        handler = DynamicPBFTHandler(-1, conf, {'host': host, 'port': port})
        log.info("create a dbft handler at %s:%s", host, port)
        asyncio.ensure_future(handler.register())
    else: 
        addr = conf['nodes'][args.index]
        host = addr['host']
        port = addr['port']
        logging_config(log_level=logging.DEBUG ,log_file='~$node_' + str(args.index)+'.log')
        log = logging.getLogger()
        # handler = PBFTHandler(args.index, conf)
        handler = SPBFTHandler(args.index, conf)
        asyncio.ensure_future(handler.score())

    log.debug(conf)

    # asyncio.ensure_future(pbft.synchronize())
    # asyncio.ensure_future(pbft.garbage_collection())

    app = web.Application()
    app.add_routes([
        web.post('/' + MessageType.REQUEST, handler.get_request),
        web.post('/' + MessageType.PREPREPARE, handler.preprepare),
        web.post('/' + MessageType.PREPARE, handler.prepare),
        web.post('/' + MessageType.COMMIT, handler.commit),
        web.post('/' + MessageType.REPLY, handler.reply),
        web.post('/' + MessageType.FEEDBACK, handler.feedback),
        web.post('/' + MessageType.CONFIRM, handler.confirm),
        web.post('/' + MessageType.FAST_REPLY, handler.fast_reply),
        web.post('/' + MessageType.RECEIVE_CKPT_VOTE, handler.receive_ckpt_vote),
        web.post('/' + MessageType.RECEIVE_SYNC, handler.receive_sync),
        web.post('/' + MessageType.VIEW_CHANGE_REQUEST, handler.get_view_change_request),
        web.post('/' + MessageType.VIEW_CHANGE_VOTE, handler.receive_view_change_vote),
        web.post('/' + MessageType.ELECT, handler.elect),
        web.post('/' + MessageType.NEW_LEADER, handler.new_leader),
        web.get('/'+'blockchain', handler.show_blockchain),
        ])


    web.run_app(app, host=host, port=port, access_log=None)


if __name__ == "__main__":
    main()

