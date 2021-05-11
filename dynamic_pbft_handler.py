import logging, traceback
import time
from random import random, randint
import aiohttp
from aiohttp import web
import asyncio

from constants import MessageType
from pbft_handler import PBFTHandler

class DynamicPBFTHandler(PBFTHandler):
    def __init__(self, index, conf, node):
        super().__init__(index, conf)
        self._node = node
        self._ca = conf['ca']

    async def register(self):
        """
        post { 'host':xx, 'port': xx }to ca to register, get index and current nodes
        """     
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        resp = await self._session.post(self.make_url(self._ca, MessageType.REGISTER), json=self._node)
        resp = await resp.json()
        self._index = resp['index']
        self._nodes = resp['nodes']
        self._log.info('register to ca, get index %d, current nodes: %s', self._index, self._nodes)

    async def join_request(self, request):
        """
        broadcast join_req to other replicas
        join_req:{
            'index': n
            'node': {
                'host': localhost,
                'port': xxx
            }
            'type': join_request
        }
        """ 
        join_req = {
            'index': self._index,
            'node': self._node,
            'type': MessageType.JOIN_REQUEST
        }
        await self._post(self._nodes, MessageType.JOIN, join_req)

    async def join(self, request):
        """
        handle join_request, broadcast join message(similar to view_change)

        Args:
            request : json format join_request
        broadcast join message:
        join_msg = {
                'index': self._index,
                'view': self._n,
                "checkpoint":self._ckpt.get_ckpt_info(),
                "prepare_certificates":await self.get_prepare_certificates(),
        }
        """   
        join_msg = {
                "index": self._index,
                "view_number": self._follow_view.get_view(),
                "checkpoint":self._ckpt.get_ckpt_info(),
                "prepare_certificates":await self.get_prepare_certificates(),
                'type': MessageType.JOIN
        }
        await self._post(self._nodes, MessageType.VIEW_CHANGE_VOTE, join_msg)







    
