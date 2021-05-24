from aiohttp.helpers import NO_EXTENSIONS
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
from spbft_handler import SPBFTHandler
from checkpoint import CheckPoint

class DynamicPBFTHandler(SPBFTHandler):
    def __init__(self, index, conf, node=None):
        self._ca = conf['ca']
        self._join_status = None
        # dynamic add from outside instead of reading from configuration
        if node != None:
            self._node = node
        super().__init__(index, conf)



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

        self._node_cnt = len(self._nodes)
        self._f = (self._node_cnt - 1) // 3
        self._is_leader = False
        self._ckpt = CheckPoint(self._checkpoint_interval, self._nodes, 
            self._f, self._index, self._loss_rate, self._network_timeout)

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
        # broadcast to others except itself
        await self._post(self._nodes[ : -1], MessageType.JOIN, join_req)


    async def join(self, request):
        """
        handle join_request, broadcast join message(similar to preprepare)

        Args:
            request : json format join_request
        broadcast join message:
        join_msg = {
                'index': self._index,
                'view': self._view.get_view(),
                'proposal': {
                        'index': n
                        'node': {
                                'host': localhost
                                'port': xxx
                        }
                        'type': join_request
                }
                'type': MessageType.JOIN,
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
        join_accept_msg = {
                'index': self._index,
                'view': self._view.get_view(),
                'proposal': {
                        'index': n
                        'node': {
                                'host': localhost
                                'port': xxx
                        }
                        'type': join_request
                }
                'type': MessageType.JOIN_ACCEPT,
            }
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
                'proposal': json_data['proposal'],
                'type': MessageType.JOIN_ACCEPT,
            }
            await self._post(self._nodes, MessageType.JOIN_REPLY, join_accept_msg)
        return web.Response()


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
            new_node = json_data['proposal']['node']
            self._nodes.append(new_node)
            self._node_cnt = len(self._nodes)
            self._f = (self._node_cnt - 1) // 3
            self._log.info("receive 2f + 1 join accept messages! update local node list to: %s", str(self._nodes))
            join_reply_msg = {
                'index': self._index,
                'view': self._view.get_view(),
                'proposal': json_data['proposal'],
                'type': MessageType.JOIN_REPLY,
                'sync': {
                    'leader': self._leader,
                    'view': self._view.get_view(),
                    'next_propose_slot': self._next_propose_slot,
                    'blocks': [block.__dict__ for block in self._blockchain.chain[1:]]
                }
            }
            await self._post([new_node], MessageType.GET_JOIN_REPLY, join_reply_msg)
        return web.Response()

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
            self._log.info("receive f + 1 join_accept messages, start syncing! %s", str(json_data))
            self._leader = json_data['sync']['leader']
            self._view = View(json_data['sync']['view'], self._node_cnt)
            self._follow_view = View(json_data['sync']['view'], self._node_cnt)
            self._next_propose_slot = json_data['sync']['next_propose_slot']
            self._last_commit_slot = self._next_propose_slot - 1
            self._blockchain.update(json_data['sync']['blocks'])
            self._log.info("update leader=%d, view=%d, slot=%d", self._leader, self._view.get_view(), self._next_propose_slot)
            with open("~$node_{}.blockchain".format(self._index), 'a') as f:
                self._log.debug("write block from %d to %d", self._blockchain.commit_counter, self._blockchain.length)
                for i in range(self._blockchain.commit_counter, self._blockchain.length):
                    f.write(str(self._blockchain.chain[i].get_json())+'\n------------\n')
                    self._blockchain.update_commit_counter()
        return web.Response()
            

        


    
