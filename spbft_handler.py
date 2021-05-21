from aiohttp import web
import aiohttp
import asyncio

from constants import MessageType
from pbft_handler import PBFTHandler
from view import View
from status import Status
from constants import MessageType
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
        self._log.debug('self._candidates %s', self._candidates)
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

    async def feedback(self, request):
        '''
        Once receive preprepare message from leader, send feedback to leader in return.
        similar to prepare
        input: 
            request: preprepare message from leader:
                preprepare_msg = {
                    'leader': self._index,
                    'view': self._view.get_view(),
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'preprepare'
                }

        '''
        json_data = await request.json()

        if json_data['view'] < self._follow_view.get_view():
            # when receive message with view < follow_view, do nothing
            return web.Response()

        # self._log.info("%d: on feedback", self._index)
        self._log.info("%d: receive preprepare msg from %d", 
            self._index, json_data['leader'])


        for slot in json_data['proposal']:

            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)

            feedback_msg = {
                'index': self._index,
                'view': json_data['view'],
                'proposal': {
                    slot: json_data['proposal'][slot]
                },
                'type': MessageType.FEEDBACK
            }
            # require leader to confirm, only send to leader!!!
            await self._post([self._nodes[self._leader]], MessageType.CONFIRM, feedback_msg)
        return web.Response()

    async def confirm(self, request):
        '''
        Once receive 3f + 1 message from replica, send confirm in return
        similar to commit

        input: 
            request: feedback message from replica
                feedback_msg = {
                    'leader': self._index,
                    'view': self._view.get_view(),
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'feedback'
                }

        '''
        json_data = await request.json()
        # self._log.info("%d: on confirm", self._index)
        self._log.info("%d: receive feedback msg from %d", 
            self._index, json_data['index'])

        if json_data['view'] < self._follow_view.get_view():
            return web.Response()
        
        for slot in json_data['proposal']:
            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View(json_data['view'], self._node_cnt)

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])

            if status._check_majority(json_data['type']):
                status.feedback_certificate = Status.Certificate(view, 
                    json_data['proposal'][slot])
                confirm_msg = {
                    'index': self._index,
                    'view': json_data['view'],
                    'proposal': {
                        slot: json_data['proposal'][slot]
                    },
                    'type': MessageType.CONFIRM
                }
                # send feedback msg, require replica to fast_reply
                await self._post(self._nodes, MessageType.FAST_REPLY, confirm_msg)
        return web.Response()

    async def fast_reply(self, request):
        '''
        Once receive confirm message from leader, write to blockchain and reply to client
        no need to save confirm messages
        input:
            request: confirm message from leader:
                confirm_msg = {
                    'index': self._index,
                    'view': ,
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'confirm'
                }
        '''
        
        json_data = await request.json()
        # self._log.info("%d: on fast_reply", self._index)

        if json_data['view'] < self._follow_view.get_view():
            return web.Response()

        self._log.info("%d: receive confirm msg from %d", 
            self._index, json_data['index'])

        for slot in json_data['proposal']:
            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View(json_data['view'], self._node_cnt)

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])


            # Directly Commit, no need to check 2f + 1
            if not status.commit_certificate:
                status.commit_certificate = Status.Certificate(view, 
                    json_data['proposal'][slot])

                self._log.debug("Add commit certifiacte to slot %d", int(slot))
                
                if self._last_commit_slot == int(slot) - 1 and not status.is_committed:

                    # client doesn't distinguish fast_reply or reply, so send type REPLY for convenience
                    fast_reply_msg = {
                        'index': self._index,
                        'view': json_data['view'],
                        'proposal': json_data['proposal'][slot],
                        'type': MessageType.REPLY
                    }
                    status.is_committed = True
                    self._last_commit_slot += 1

                    # When commit messages fill the next checkpoint, 
                    # propose a new checkpoint.
                    if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                        await self._ckpt.propose_vote(self.get_commit_decisions())
                        self._log.info("%d: Propose checkpoint with last slot: %d. "
                            "In addition, current checkpoint's next_slot is: %d", 
                            self._index, self._last_commit_slot, self._ckpt.next_slot)


                    # Write to blockchain
                    if (self._last_commit_slot + 1) % self._dump_interval == 0:
                        await self.dump_to_file()
                    # reply to client
                    try:
                        await self._session.post(
                            json_data['proposal'][slot]['client_url'], json=fast_reply_msg)
                        self._score += 1
                        self._log.info("reply to client successfully, score + 1, current score = %d", self._score)
                    except:
                        self._log.error("Send message failed to %s", 
                            json_data['proposal'][slot]['client_url'])
                        pass
                    else:
                        self._log.info("%d fast reply to %s successfully!!", 
                            self._index, json_data['proposal'][slot]['client_url'])
                
        return web.Response()

    