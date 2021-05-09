#! /usr/bin/env python3
import logging, traceback
import argparse
import yaml
from collections import Counter
import asyncio
from aiohttp import web

from pbft_handler import PBFTHandler

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
    args = parser.parse_args()
    return args

def conf_parse(conf_file) -> dict:
    '''
    nodes:
        - host: localhost
          port: 30000
        - host: localhost
          port: 30001
        - host: localhost
          port: 30002
        - host: localhost
          port: 30003

    clients:
        - host: localhost
          port: 20001
        - host: localhost
          port: 20002

    loss%: 0

    ckpt_interval: 10

    retry_times_before_view_change: 2

    sync_interval: 5

    misc:
        network_timeout: 5
    '''
    conf = yaml.safe_load(conf_file)
    return conf


    

def main():
    args = arg_parse()
    # if args.log_to_file:
    #     logging.basicConfig(filename='~$node_' + str(args.index)+'.log',
    #                         filemode='a', level=logging.DEBUG)
    logging_config(log_level=logging.DEBUG ,log_file='~$node_' + str(args.index)+'.log')
    log = logging.getLogger()
    conf = conf_parse(args.config)
    log.debug(conf)

    addr = conf['nodes'][args.index]
    host = addr['host']
    port = addr['port']

    pbft = PBFTHandler(args.index, conf)

    # asyncio.ensure_future(pbft.synchronize())
    asyncio.ensure_future(pbft.garbage_collection())

    app = web.Application()
    app.add_routes([
        web.post('/' + PBFTHandler.REQUEST, pbft.get_request),
        web.post('/' + PBFTHandler.PREPREPARE, pbft.preprepare),
        web.post('/' + PBFTHandler.PREPARE, pbft.prepare),
        web.post('/' + PBFTHandler.COMMIT, pbft.commit),
        web.post('/' + PBFTHandler.REPLY, pbft.reply),
        web.post('/' + PBFTHandler.RECEIVE_CKPT_VOTE, pbft.receive_ckpt_vote),
        web.post('/' + PBFTHandler.RECEIVE_SYNC, pbft.receive_sync),
        web.post('/' + PBFTHandler.VIEW_CHANGE_REQUEST, pbft.get_view_change_request),
        web.post('/' + PBFTHandler.VIEW_CHANGE_VOTE, pbft.receive_view_change_vote),
        web.get('/'+'blockchain', pbft.show_blockchain),
        ])




    web.run_app(app, host=host, port=port, access_log=None)


if __name__ == "__main__":
    main()

