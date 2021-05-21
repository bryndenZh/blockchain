from blockchain_client import Status
import json
import logging, traceback
import time
from random import random, randint
import aiohttp
from aiohttp import web
import asyncio

from constants import MessageType
from view import View
from status import Status
from pbft_handler import PBFTHandler

class DynamicPBFTHandler(PBFTHandler):
    def __init__(self, index, conf, node):
        super().__init__(index, conf)
        # dynamic add from outside instead of reading from configuration
        self._node = node
        self._ca = conf['ca']
        self._join_status = None

    async def register(self):
        """
        post { 'host':xx, 'port': xx }to ca to register, get index and current nodes. then broadcast join_request
        """     
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        resp = await self._session.post(self.make_url(self._ca, MessageType.REGISTER), json=self._node)
        resp = await resp.json()
        self._index = resp['index']
        self._nodes = resp['nodes']
        self._log.info('register to ca, get index %d, current nodes: %s', self._index, self._nodes)
        await self.join_request()


    async def join_request(self):
        """
        broadcast join_req to other replicas
        similar to preprepare
        join_req:{
            'index': n
            'node': {
                'host': localhost
                'port': xxx
            }
            'type': join_request
        }
        """ 
        node = self._node.copy()
        node.update({'index': self._index})
        join_req = {
            'index': self._index,
            'node': self._node,
            'type': MessageType.JOIN_REQUEST
        }
        await self._post(self._nodes, MessageType.JOIN, join_req)


    async def join(self, request):
        """
        handle join_request, broadcast join message(similar to preprepare)

        Args:
            request : json format join_request
        broadcast join message:
        join_msg = {
                'index': self._index,
        }
        """   
        json_data = await request.json()
        self._log.info("receive join request for node: %s", str(json_data))
        if self._join_status == None:
            self._join_status = Status(self._f)
        self._join_status.request = json_data
        join_msg = {
                'index': self._index,
                'view': self._view.get_view(),
                'proposal': json_data,
                'type': MessageType.JOIN,
        }
        await self._post(self._nodes, MessageType.JOIN_ACCEPT, join_msg)
        return web.Response()


    async def join_accept(self, request):
        '''
        handle join message. if 2f + 1, broadcast join_accept message(similar to commit)
        '''
        json_data = await request.json()
        self._log.info("receive join message from %i", json_data['index'])
        if self._join_status == None:
            self._join_status = Status(self._f)
        view = View(json_data['view'], self._node_cnt)
        self._join_status._update_sequence(json_data['type'], 
            view, json_data['proposal'], json_data['index'])
        # only broadcast once
        if not self._join_status.is_joined and self._join_status._check_majority(json_data['type']):
            join_accept_msg = {
                'index': self._index,
                'view': self._view.get_view(),
                'proposal': json_data,
                'type': MessageType.JOIN_ACCEPT,
            }
            await self._post(self._nodes, MessageType.JOIN_REPLY, join_accept_msg)

    async def join_reply(self, request):
        '''
        handle join accept message. if 2f + 1, reply to join-node
        '''
        json_data = await request.json()
        self._log.info("receive join accept message from %i", json_data['index'])
        if self._join_status == None:
            self._join_status = Status(self._f)
        view = View(json_data['view'], self._node_cnt)
        self._join_status._update_sequence(json_data['type'], 
            view, json_data['proposal'], json_data['index'])
        if not self._join_status.is_join_accepted and self._join_status._check_majority(json_data['type']):
            join_reply_msg = {
                'index': self._index,
                'view': self._view.get_view(),
                'proposal': json_data,
                'type': MessageType.JOIN_REPLY,
            }
            await self._post(self._nodes, MessageType.GET_JOIN_REPLY, join_reply_msg)

    async def get_join_reply(self, request):
        '''
        handle join reply message. if f + 1, join success
        '''
        json_data = await request.json()
        self._log.info("receive join reply message from %i", json_data['index'])
        if self._join_status == None:
            self._join_status = Status(self._f)
        view = View(json_data['view'], self._node_cnt)
        self._join_status._update_sequence(json_data['type'], 
            view, json_data['proposal'], json_data['index'])
        if not self._join_status.get_enough_join_reply and self._join_status._check_majority(json_data['type']):
            self._log.info("receive f + 1 join_accept messages, start syncing!")
            

        


    
