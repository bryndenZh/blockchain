import logging
import hashlib
import json
from random import randint, random
import aiohttp
from aiohttp import web

class CheckPoint:
    '''
    Record all the status of the checkpoint for given PBFTHandler.
    '''
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'
    def __init__(self, checkpoint_interval, nodes, f, node_index, 
            lose_rate = 0, network_timeout = 10):
        self._checkpoint_interval = checkpoint_interval
        self._nodes = nodes
        self._f = f
        self._node_index = node_index
        self._loss_rate = lose_rate
        self._log = logging.getLogger(__name__) 
        # Next slot of the given globally accepted checkpoint.
        # For example, the current checkpoint record until slot 99
        # next_slot = 100
        self.next_slot = 0
        # Globally accepted checkpoint
        self.checkpoint = []
        # Use the hash of the checkpoint to record receive votes for given ckpt.
        self._received_votes_by_ckpt = {} 
        self._session = None
        self._network_timeout = network_timeout

        self._log.info("---> %d: Create checkpoint.", self._node_index)

    # Class to record the status of received checkpoints
    class ReceiveVotes:
        def __init__(self, ckpt, next_slot):
            self.from_nodes = set([])
            self.checkpoint = ckpt
            self.next_slot = next_slot

    def get_commit_upperbound(self):
        '''
        Return the upperbound that could commit 
        (return upperbound = true upperbound + 1)
        '''
        return self.next_slot + 2 * self._checkpoint_interval

    def _hash_ckpt(self, ckpt):
        '''
        input: 
            ckpt: the checkpoint
        output:
            The hash of the input checkpoint in the format of 
            binary string.
        '''
        hash_object = hashlib.sha256(json.dumps(ckpt, sort_keys=True).encode())
        return hash_object.digest()  


    async def receive_vote(self, ckpt_vote):
        '''
        Trigger when PBFTHandler receive checkpoint votes.
        First, we update the checkpoint status. Second, 
        update the checkpoint if more than 2f + 1 node 
        agree with the given checkpoint.
        input: 
            ckpt_vote = {
                'node_index': self._node_index
                'next_slot': self._next_slot + self._checkpoint_interval
                'ckpt': json.dumps(ckpt)
                'type': 'vote'
            }
        '''
        self._log.debug("---> %d: Receive checkpoint votes", self._node_index)
        ckpt = json.loads(ckpt_vote['ckpt'])
        next_slot = ckpt_vote['next_slot']
        from_node = ckpt_vote['node_index']

        hash_ckpt = self._hash_ckpt(ckpt)
        if hash_ckpt not in self._received_votes_by_ckpt:
            self._received_votes_by_ckpt[hash_ckpt] = (
                CheckPoint.ReceiveVotes(ckpt, next_slot))
        status = self._received_votes_by_ckpt[hash_ckpt]
        status.from_nodes.add(from_node)
        for hash_ckpt in self._received_votes_by_ckpt:
            if (self._received_votes_by_ckpt[hash_ckpt].next_slot > self.next_slot and 
                    len(self._received_votes_by_ckpt[hash_ckpt].from_nodes) >= 2 * self._f + 1):
                self._log.info("---> %d: Update checkpoint by receiving votes", self._node_index)
                self.next_slot = self._received_votes_by_ckpt[hash_ckpt].next_slot
                self.checkpoint = self._received_votes_by_ckpt[hash_ckpt].checkpoint
               


    async def propose_vote(self, commit_decisions):
        '''
        When node the slots of committed message exceed self._next_slot 
        plus self._checkpoint_interval, propose new checkpoint and 
        broadcast to every node

        input: 
            commit_decisions: list of tuple: [((client_index, client_seq), data), ... ]

        output:
            next_slot for the new update and garbage collection of the Status object.
        '''
        proposed_checkpoint = self.checkpoint + commit_decisions
        json_data = {
            'node_index': self._node_index,
            'next_slot': self.next_slot + self._checkpoint_interval,
            'ckpt': json.dumps(proposed_checkpoint),
            'type': 'vote'
        }
        await self._post(self._nodes, CheckPoint.RECEIVE_CKPT_VOTE, json_data)


    async def _post(self, nodes, command, json_data):
        '''
        Broadcast json_data to all node in nodes with given command.
        input:
            nodes: list of nodes
            command: action
            json_data: Data in json format.
        '''
        if not self._session:
            timeout = aiohttp.ClientTimeout(self._network_timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                self._log.debug("make request to %d, %s", i, json_data['type'])
                try:
                    _ = await self._session.post(
                        self.make_url(node, command), json=json_data)
                except Exception as e:
                    #resp_list.append((i, e))
                    self._log.error(e)
                    pass

    @staticmethod
    def make_url(node, command):
        '''
        input: 
            node: dictionary with key of host(url) and port
            command: action
        output:
            The url to send with given node and action.
        '''
        return "http://{}:{}/{}".format(node['host'], node['port'], command)



    def get_ckpt_info(self):

        '''
        Get the checkpoint serialized information.Called 
        by synchronize function to get the checkpoint
        information.
        '''
        json_data = {
            'next_slot': self.next_slot,
            'ckpt': json.dumps(self.checkpoint)
        }
        return json_data

    def update_checkpoint(self, json_data):
        '''
        Update the checkpoint when input checkpoint cover 
        more slots than current.
        input: 
            json_data = {
                'next_slot': self._next_slot
                'ckpt': json.dumps(ckpt)
            }     
        '''
        self._log.debug("update_checkpoint: next_slot: %d; update_slot: %d"
            , self.next_slot, json_data['next_slot'])
        if json_data['next_slot'] > self.next_slot:
            self._log.info("---> %d: Update checkpoint by synchronization.", self._node_index)
            self.next_slot = json_data['next_slot']
            self.checkpoint = json.loads(json_data['ckpt'])
        

    async def receive_sync(sync_ckpt):
        '''
        Trigger when recieve checkpoint synchronization messages.
        input: 
            sync_ckpt = {
                'node_index': self._node_index
                'next_slot': self._next_slot + self._checkpoint_interval
                'ckpt': json.dumps(ckpt)
                'type': 'sync'
            }
        '''
        self._log.debug("receive_sync in checkpoint: current next_slot:"
            " %d; update to: %d" , self.next_slot, json_data['next_slot'])

        if sync_ckpt['next_slot'] > self._next_slot:
            self.next_slot = sync_ckpt['next_slot']
            self.checkpoint = json.loads(sync_ckpt['ckpt'])

    async def garbage_collection(self):
        '''
        Clean those ReceiveCKPT objects whose next_slot smaller
        than or equal to the current.
        '''
        deletes = []
        for hash_ckpt in self._received_votes_by_ckpt:
            if self._received_votes_by_ckpt[hash_ckpt].next_slot <= next_slot:
                deletes.append(hash_ckpt)
        for hash_ckpt in deletes:
            del self._received_votes_by_ckpt[hash_ckpt]

