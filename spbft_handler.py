from aiohttp import web
import aiohttp
import asyncio

from constants import MessageType
from pbft_handler import PBFTHandler
class SPBFTHandler(PBFTHandler):
    def __init__(self, index, conf):
        super().__init__(index, conf)
        self._score = conf['nodes'][self._index]['score']
        # node score by their index in this round
        self._candidates = {}
        # max score chosen in elect stage
        self._temp_leader = -1
        # node's received vote count by their index
        self._leader_votes = {}
        # whether this round has finished
        self._elected = False


    async def score(self):
        '''
        broadcast score to others, require them to elect. Trigger by client when fault occurs
        msg = {
            'index': self._index
            'score': self._score
            'type': score
        }
        '''
        # wait other replicas to start up
        await asyncio.sleep(self._sync_interval)
        # each time start a elect process, clear history candidates
        self._elected = False
        msg = {
            'index': self._index,
            'score': self._score,
            'type': MessageType.SCORE
        }
        await self._post(self._nodes, MessageType.ELECT, msg)

    async def elect(self, request):
        '''
        compare score from others
        when receive all msg, broadcast new leader msg
        '''
        if self._elected:
            return web.Response()
        msg = await request.json()
        self._log.info("receive score msg: %s", str(msg))
        self._candidates[msg['index']] = msg['score']
        self._log.info('self._candidates %s', self._candidates)
        if self._temp_leader == -1:
            self._temp_leader = msg['index']
        elif msg['score'] > self._candidates[self._temp_leader] or (msg['score'] == self._candidates[self._temp_leader] and msg['index'] < self._temp_leader):
            self._temp_leader = msg['index']
        if len(self._candidates) == len(self._nodes):
            self._log.info('receive all score meesages, elect %d to be leader', self._temp_leader)
            new_leader = {
                'index': self._index,
                'leader': self._temp_leader,
                'leader_score': self._candidates[self._temp_leader],
                'type': MessageType.ELECT
            }
            await self._post(self._nodes, MessageType.NEW_LEADER, new_leader)
        return web.Response()
    

    async def new_leader(self, request):
        '''
        when receive 2f + 1 leader message, update leader 
        '''
        
        if self._elected:
            return web.Response()
        leader_vote = await request.json()
        self._log.debug("receive leader msg: %s", str(leader_vote))
        leader = leader_vote['leader']
        if leader not in self._leader_votes:
            self._leader_votes[leader] = []
        self._leader_votes[leader].append(leader_vote)
        if len(self._leader_votes[leader]) == 2 * self._f + 1:
            self._leader = leader
            self._log.info("%d is elected to be leader", self._leader)
            if self._index == self._leader:
                self._is_leader = True
            self._elected = True
            self._leader_votes = []
            
        return web.Response()

        
    