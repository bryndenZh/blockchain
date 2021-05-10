import logging, traceback
import time
from random import random, randint
import aiohttp
from aiohttp import web
import asyncio


from view import View, ViewChangeVotes
from blockchain import Blockchain, Block
from checkpoint import CheckPoint
from status import Status
from constants import MessageType

class PBFTHandler:
    # REQUEST = MessageType.REQUEST
    # PREPREPARE = MessageType.PREPREPARE
    # PREPARE = MessageType.PREPARE
    # COMMIT = MessageType.COMMIT
    # REPLY = MessageType.REPLY
    
    # FEEDBACK = MessageType.FEEDBACK
    # CONFIRM = MessageType.CONFIRM
    # FAST_REPLY = MessageType.FAST_REPLY

    # NO_OP = MessageType.NO_OP

    # RECEIVE_SYNC = MessageType.RECEIVE_SYNC
    # RECEIVE_CKPT_VOTE = MessageType.RECEIVE_CKPT_VOTE

    # VIEW_CHANGE_REQUEST = MessageType.VIEW_CHANGE_REQUEST
    # VIEW_CHANGE_VOTE = MessageType.VIEW_CHANGE_VOTE

    def __init__(self, index, conf):
        self._nodes = conf['nodes']
        self._node_cnt = len(self._nodes)
        self._index = index
        # Number of faults tolerant.
        self._f = (self._node_cnt - 1) // 3

        # leader
        self._view = View(0, self._node_cnt)
        self._next_propose_slot = 0

        self._blockchain =  Blockchain()

        # tracks if commit_decisions had been commited to blockchain
        self.committed_to_blockchain = False

        # TODO: Test fixed
        if self._index == 0:
            self._is_leader = True
        else:
            self._is_leader = False

        # Network simulation
        self._loss_rate = conf['loss%'] / 100

        # Time configuration
        self._network_timeout = conf['misc']['network_timeout']

        # Checkpoint

        # After finishing committing self._checkpoint_interval slots,
        # trigger to propose new checkpoint.
        self._checkpoint_interval = conf['ckpt_interval']
        self._ckpt = CheckPoint(self._checkpoint_interval, self._nodes, 
            self._f, self._index, self._loss_rate, self._network_timeout)
        # Commit
        self._last_commit_slot = -1

        self._dump_interval = conf['dump_interval']
        # Indicate my current leader.
        # TODO: Test fixed
        self._leader = 0

        # The largest view either promised or accepted
        self._follow_view = View(0, self._node_cnt)
        # Restore the votes number and information for each view number
        self._view_change_votes_by_view_number = {}
        
        # Record all the status of the given slot
        # To adjust json key, slot is string integer.
        self._status_by_slot = {}

        self._sync_interval = conf['sync_interval']
 
        
        self._session = None
        self._log = logging.getLogger(__name__) 


            
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

    async def _make_requests(self, nodes, command, json_data):
        '''
        Send json data:

        input:
            nodes: list of dictionary with key: host, port
            command: Command to execute.
            json_data: Json data.
        output:
            list of tuple: (node_index, response)

        '''
        resp_list = []
        for i, node in enumerate(nodes):
            if random() > self._loss_rate:
                if not self._session:
                    timeout = aiohttp.ClientTimeout(self._network_timeout)
                    self._session = aiohttp.ClientSession(timeout=timeout)
                self._log.debug("make request to %d, %s", i, command)
                try:
                    resp = await self._session.post(self.make_url(node, command), json=json_data)
                    resp_list.append((i, resp))
                    
                except Exception as e:
                    #resp_list.append((i, e))
                    self._log.error(e)
                    pass
        return resp_list 

    async def _make_response(self, resp):
        '''
        Drop response by chance, via sleep for sometime.
        '''
        if random() < self._loss_rate:
            await asyncio.sleep(self._network_timeout)
        return resp

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
                self._log.debug("make request to %d, %s", i, json_data['type'] )
                try:
                    _ = await self._session.post(self.make_url(node, command), json=json_data)
                except Exception as e:
                    #resp_list.append((i, e))
                    self._log.error(e)
                    pass

    def _legal_slot(self, slot):
        '''
        the slot is legal only when it's between upperbound and the lowerbound.
        input:
            slot: string integer direct get from the json_data proposal key.
        output:
            boolean to express the result.
        '''
        if int(slot) < self._ckpt.next_slot or int(slot) >= self._ckpt.get_commit_upperbound():
            return False
        else:
            return True
    
    async def get_request(self, request):
        '''
        Handle the request from client if leader, otherwise 
        redirect to the leader.
        '''
        self._log.info("%d: on request", self._index)

        if not self._is_leader:
            if self._leader != None:
                raise web.HTTPTemporaryRedirect(self.make_url(
                    self._nodes[self._leader], MessageType.REQUEST))
            else:
                raise web.HTTPServiceUnavailable()
        else:

            # print(request.headers)
            # print(request.__dict__)

            json_data = await request.json()


            # print("\t\t--->node"+str(self._index)+": on request :")
            # print(json_data)

            await self.preprepare(json_data)
            return web.Response()


    async def preprepare(self, json_data):
        '''
        Prepare: Deal with request from the client and broadcast to other replicas.
        input:
            json_data: Json-transformed web request from client
                {
                    id: (client_id, client_seq),
                    client_url: "url string"
                    timestamp:"time"
                    data: "string"
                }

        '''

        this_slot = str(self._next_propose_slot)
        self._next_propose_slot = int(this_slot) + 1

        self._log.info("%d: on preprepare, propose at slot: %d", 
            self._index, int(this_slot))

        if this_slot not in self._status_by_slot:
            self._status_by_slot[this_slot] = Status(self._f)
        self._status_by_slot[this_slot].request = json_data

        preprepare_msg = {
            'leader': self._index,
            'view': self._view.get_view(),
            'proposal': {
                this_slot: json_data
            },
            'type': 'preprepare'
        }
        
        # await self._post(self._nodes, MessageType.PREPARE, preprepare_msg)
        # require replicas to feedback instead of prepare
        await self._post(self._nodes, MessageType.FEEDBACK, preprepare_msg)

    # similar to prepare
    async def feedback(self, request):
        '''
        Once receive preprepare message from leader, send feedback to leader in return.
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

        self._log.info("%d: on feedback", self._index)
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

    # similar to commit 
    async def confirm(self, request):
        '''
        Once receive 3f + 1 message from replica, send confirm in return

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
        self._log.info("%d: on confirm", self._index)
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
        Once receive confirm message from leader, append the commit 
        certificate and cannot change anymore. In addition, if there is 
        no bubbles ahead, commit the given slots and update the last_commit_slot.
        input:
            request: confirm message from leader:
                confirm_msg = {
                    'index': self._index,
                    'n': self._n,
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'confirm'
                }
        '''
        
        json_data = await request.json()
        self._log.info("%d: on fast_reply", self._index)

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
                    # if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                    #     await self._ckpt.propose_vote(self.get_commit_decisions())
                    #     self._log.info("%d: Propose checkpoint with last slot: %d. "
                    #         "In addition, current checkpoint's next_slot is: %d", 
                    #         self._index, self._last_commit_slot, self._ckpt.next_slot)


                    # Write to blockchain
                    if (self._last_commit_slot + 1) % self._dump_interval == 0:
                        await self.dump_to_file()
                    try:
                        await self._session.post(
                            json_data['proposal'][slot]['client_url'], json=fast_reply_msg)
                    except:
                        self._log.error("Send message failed to %s", 
                            json_data['proposal'][slot]['client_url'])
                        pass
                    else:
                        self._log.info("%d fast reply to %s successfully!!", 
                            self._index, json_data['proposal'][slot]['client_url'])
                
        return web.Response()

    async def prepare(self, request):
        '''
        Once receive preprepare message from leader, broadcast 
        prepare message to all replicas.

        input: 
            request: preprepare message from preprepare:
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

        self._log.info("%d: on prepare", self._index)
        self._log.info("%d: receive preprepare msg from %d", 
            self._index, json_data['leader'])


        for slot in json_data['proposal']:

            if not self._legal_slot(slot):
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)

            prepare_msg = {
                'index': self._index,
                'view': json_data['view'],
                'proposal': {
                    slot: json_data['proposal'][slot]
                },
                'type': MessageType.PREPARE
            }
            await self._post(self._nodes, MessageType.COMMIT, prepare_msg)
        return web.Response()

    async def commit(self, request):
        '''
        Once receive more than 2f + 1 prepare message,
        send the commit message.
        input:
            request: prepare message from prepare:
                prepare_msg = {
                    'index': self._index,
                    'view': self._n,
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'prepare'
                }
        '''
        json_data = await request.json()
        self._log.info("%d: on commit", self._index)
        self._log.info("%d: receive prepare msg from %d", 
            self._index, json_data['index'])

        # print("\t--->node "+str(self._index)+": receive prepare msg from node "+str(json_data['index']))
        # print(json_data)

        if json_data['view'] < self._follow_view.get_view():
            # when receive message with view < follow_view, do nothing
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
                status.prepare_certificate = Status.Certificate(view, 
                    json_data['proposal'][slot])
                commit_msg = {
                    'index': self._index,
                    'view': json_data['view'],
                    'proposal': {
                        slot: json_data['proposal'][slot]
                    },
                    'type': MessageType.COMMIT
                }
                await self._post(self._nodes, MessageType.REPLY, commit_msg)
        return web.Response()

    async def reply(self, request):
        '''
        Once receive more than 2f + 1 commit message, append the commit 
        certificate and cannot change anymore. In addition, if there is 
        no bubbles ahead, commit the given slots and update the last_commit_slot.
        input:
            request: commit message from commit:
                preprepare_msg = {
                    'index': self._index,
                    'n': self._n,
                    'proposal': {
                        this_slot: json_data
                    }
                    'type': 'commit'
                }
        '''
        
        json_data = await request.json()
        self._log.info(" %d: on reply", self._index)
        # print("\t--->node "+str(self._index)+": on reply ")

        if json_data['view'] < self._follow_view.get_view():
            # when receive message with view < follow_view, do nothing
            return web.Response()

        self._log.info(" %d: receive commit msg from %d", 
            self._index, json_data['index'])

        for slot in json_data['proposal']:
            if not self._legal_slot(slot):
                self._log.error("%d: message %s not in valid slot", self._index, json_data)
                continue

            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
            status = self._status_by_slot[slot]

            view = View(json_data['view'], self._node_cnt)

            status._update_sequence(json_data['type'], 
                view, json_data['proposal'][slot], json_data['index'])

            # Commit only when no commit certificate and got more than 2f + 1
            if not status.commit_certificate and status._check_majority(json_data['type']):
                status.commit_certificate = Status.Certificate(view, 
                    json_data['proposal'][slot])

                self._log.debug("Add commit certifiacte to slot %d", int(slot))
                
                # Reply only once and only when no bubble ahead
                if self._last_commit_slot == int(slot) - 1 and not status.is_committed:

                    status.is_committed = True
                    self._last_commit_slot += 1

                    # When commit messages fill the next checkpoint, 
                    # propose a new checkpoint.
                    # if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                    #     self._log.info("%d: Propose checkpoint with last slot: %d. "
                    #             "In addition, current checkpoint's next_slot is: %d", 
                    #         self._index, self._last_commit_slot, self._ckpt.next_slot)
                    #     await self._ckpt.propose_vote(self.get_commit_decisions())

                    if (self._last_commit_slot + 1) % self._dump_interval == 0:
                        await self.dump_to_file()

                    reply_msg = {
                        'index': self._index,
                        'view': json_data['view'],
                        'proposal': json_data['proposal'][slot],
                        'type': MessageType.REPLY
                    }
                    try:
                        await self._session.post(
                            json_data['proposal'][slot]['client_url'], json=reply_msg)
                    except:
                        self._log.error("Send message failed to %s", 
                            json_data['proposal'][slot]['client_url'])
                        pass
                    else:
                        self._log.info("%d reply to %s successfully!!", 
                            self._index, json_data['proposal'][slot]['client_url'])
                
        return web.Response()

    def get_commit_decisions(self):
        '''
        Get the commit decision between the next slot of the current ckpt until last commit slot.
        output:
            commit_decisions: list of tuple: [((client_index, client_seq), data), ... ]
        '''
        commit_decisions = []
        self._log.debug("get commit decisions, committed=%s, len(commit_decisions)=%d", self.committed_to_blockchain, len(commit_decisions))
        # print(self._ckpt.next_slot, self._last_commit_slot + 1)
        for i in range(self._ckpt.next_slot, self._last_commit_slot + 1):
            status = self._status_by_slot[str(i)]
            proposal = status.commit_certificate._proposal 

            commit_decisions.append((str(proposal['id']), proposal['data']))
        return commit_decisions


    async def dump_to_file(self):
        '''
        Dump the current commit decisions to disk.
        '''
        try:
            transactions = []
            self._log.debug("ready to dump, last_commit_slot = %d", self._last_commit_slot)
            for i in range(self._last_commit_slot - self._dump_interval + 1, self._last_commit_slot + 1):
                status = self._status_by_slot[str(i)]
                proposal = status.commit_certificate._proposal 
                transactions.append((str(proposal['id']), proposal['data']))
            self._log.debug("collect %d transactions", len(transactions))
            try:
                timestamp = time.asctime( time.localtime( proposal['timestamp']) )
            except Exception as e:
                self._log.error("received invalid timestamp. replacing with current timestamp")
                timestamp = time.asctime( time.localtime( time.time()) )

            new_block =  Block(self._blockchain.length, transactions, timestamp , self._blockchain.last_block_hash())
            self._blockchain.add_block(new_block)

        except Exception as e:
            traceback.print_exc()
            print(e)

        with open("~$node_{}.blockchain".format(self._index), 'a') as f:
            self._log.debug("write block %d to %d", self._blockchain.commit_counter, self._blockchain.length)
            for i in range(self._blockchain.commit_counter, self._blockchain.length):
                f.write(str(self._blockchain.chain[i].get_json())+'\n------------\n')
                self._blockchain.update_commit_counter()

        

    async def receive_ckpt_vote(self, request):
        '''
        Receive the message sent from CheckPoint.propose_vote()
        '''
        self._log.info("%d: receive checkpoint vote.", self._index)
        json_data = await request.json()

        # print ()
        # print ()
        # print ('json_data')
        # print(json_data)
        # print ()
        # print ()
        # # print ('ckpt')
        # # print (ckpt)
        # print ()

        await self._ckpt.receive_vote(json_data)
        return web.Response()

    async def receive_sync(self, request):
        '''
        Update the checkpoint and fill the bubble when receive sync messages.
        input:
            request: {
                'checkpoint': json_data = {
                    'next_slot': self._next_slot
                    'ckpt': json.dumps(ckpt)
                }
                'commit_certificates':commit_certificates
                    (Elements are commit_certificate.to_dict())
            }
        '''
        self._log.info("%d: on receive sync stage.", self._index)
        json_data = await request.json()

        try:
            # print(len(self._status_by_slot))
            # print(self._ckpt.next_slot, self._last_commit_slot + 1)
            # # print(len(json_data['checkpoint']))
            # print('node :' + str(self._index) +' > '+str(self._blockchain.commit_counter)+' : '+str(self._blockchain.length))
            # print()
            # print()
            self.committed_to_blockchain = False
        except Exception as e:
            traceback.print_exc()
            print('for i = ' +str(i))
            print(e)


        self._ckpt.update_checkpoint(json_data['checkpoint'])
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)
        # TODO: Only check bubble instead of all slots between lowerbound
        # and upperbound of the commit.

        for slot in json_data['commit_certificates']:
            # Skip those slot not qualified for update.
            if int(slot) >= self._ckpt.get_commit_upperbound() or (
                    int(slot) < self._ckpt.next_slot):
                continue

            certificate = json_data['commit_certificates'][slot]
            if slot not in self._status_by_slot:
                self._status_by_slot[slot] = Status(self._f)
                commit_certificate = Status.Certificate(View(0, self._node_cnt))
                commit_certificate.dumps_from_dict(certificate)
                self._status_by_slot[slot].commit_certificate =  commit_certificate
            elif not self._status_by_slot[slot].commit_certificate:
                commit_certificate = Status.Certificate(View(0, self._node_cnt))
                commit_certificate.dumps_from_dict(certificate)
                self._status_by_slot[slot].commit_certificate =  commit_certificate

        # Commit once the next slot of the last_commit_slot get commit certificate
        while (str(self._last_commit_slot + 1) in self._status_by_slot and 
                self._status_by_slot[str(self._last_commit_slot + 1)].commit_certificate):
            self._last_commit_slot += 1

            # When commit messages fill the next checkpoint, 
            # propose a new checkpoint.
            if (self._last_commit_slot + 1) % self._checkpoint_interval == 0:
                await self._ckpt.propose_vote(self.get_commit_decisions())

                self._log.info("%d: During rev_sync, Propose checkpoint with l "
                    "ast slot: %d. In addition, current checkpoint's next_slot is: %d", 
                    self._index, self._last_commit_slot, self._ckpt.next_slot)

        await self.dump_to_file()

        return web.Response()
        

    async def synchronize(self):
        '''
        Broadcast current checkpoint and all the commit certificate 
        between next slot of the checkpoint and commit upperbound.

        output:
            json_data = {
                'checkpoint': json_data = {
                    'next_slot': self._next_slot
                    'ckpt': json.dumps(ckpt)
                }
                'commit_certificates':commit_certificates
                    (Elements are commit_certificate.to_dict())
            }
        '''
        # TODO: Only send bubble slot message instead of all.
        while 1:
            await asyncio.sleep(self._sync_interval)
            commit_certificates = {}
            for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
                slot = str(i)
                if (slot in self._status_by_slot) and (
                        self._status_by_slot[slot].commit_certificate):
                    status = self._status_by_slot[slot]
                    commit_certificates[slot] = status.commit_certificate.to_dict()
            json_data = {
                'checkpoint': self._ckpt.get_ckpt_info(),
                'commit_certificates':commit_certificates
            }
            await self._post(self._nodes, MessageType.RECEIVE_SYNC, json_data)

    async def get_prepare_certificates(self):
        '''
        For view change, get all prepare certificates in the valid commit interval.
        output:
            prepare_certificate_by_slot: dictionary which contains the mapping between
            each slot and its prepare_certificate if exists.

        '''
        prepare_certificate_by_slot = {}
        for i in range(self._ckpt.next_slot, self._ckpt.get_commit_upperbound()):
            slot = str(i)
            if slot in self._status_by_slot:
                status = self._status_by_slot[slot]
                if status.prepare_certificate:
                    prepare_certificate_by_slot[slot] = (
                        status.prepare_certificate.to_dict())
        return prepare_certificate_by_slot

    async def _post_view_change_vote(self):
        '''
        Broadcast the view change vote messages to all the nodes. 
        View change vote messages contain current node index, 
        proposed new view number, checkpoint info, and all the 
        prepare certificate between valid slots.
        '''
        view_change_vote = {
            "node_index": self._index,
            "view_number": self._follow_view.get_view(),
            "checkpoint":self._ckpt.get_ckpt_info(),
            "prepare_certificates":await self.get_prepare_certificates(),

        }
        await self._post(self._nodes, MessageType.VIEW_CHANGE_VOTE, view_change_vote)

    async def get_view_change_request(self, request):
        '''
        Get view change request from client. Broadcast the view change vote and 
        all the information needed for view change(checkpoint, prepared_certificate)
        to every replicas.
        input:
            request: view change request messages from client.
                json_data{
                    "action" : "view change"
                }
        '''

        self._log.info("%d: receive view change request from client.", self._index)
        json_data = await request.json()
        # Make sure the message is valid.
        if json_data['action'] != "view change":
            return web.Response()
        # Update view number by 1 and change the followed leader. In addition,
        # if receive view update message within update interval, do nothing.   
        if not self._follow_view.set_view(self._follow_view.get_view() + 1):
            return web.Response()

        self._leader = self._follow_view.get_leader()
        if self._is_leader:
            self._log.info("%d is not leader anymore. View number: %d", 
                    self._index, self._follow_view.get_view())
            self._is_leader = False

        self._log.debug("%d: vote for view change to %d.", 
            self._index, self._follow_view.get_view())

        await self._post_view_change_vote()

        return web.Response()

    async def receive_view_change_vote(self, request):
        '''
        Receive the vote message for view change. (1) Update the checkpoint 
        if receive messages has larger checkpoint. (2) Update votes message 
        (Node comes from and prepare-certificate). (3) View change if receive
        f + 1 votes (4) if receive more than 2f + 1 node and is the leader 
        of the current view, become leader and preprepare the valid slot.

        input: 
            request. After transform to json:
                json_data = {
                    "node_index": self._index,
                    "view_number": self._follow_view.get_view(),
                    "checkpoint":self._ckpt.get_ckpt_info(),
                    "prepared_certificates":self.get_prepare_certificates(),
                }
        '''

        self._log.info("%d receive view change vote.", self._index)
        json_data = await request.json()
        view_number = json_data['view_number']
        if view_number not in self._view_change_votes_by_view_number:
            self._view_change_votes_by_view_number[view_number]= (
                ViewChangeVotes(self._index, self._node_cnt))


        self._ckpt.update_checkpoint(json_data['checkpoint'])
        self._last_commit_slot = max(self._last_commit_slot, self._ckpt.next_slot - 1)

        votes = self._view_change_votes_by_view_number[view_number]

        votes.receive_vote(json_data)

        # Receive more than 2f + 1 votes. If the node is the 
        # charged leader for current view, become leader and 
        # propose preprepare for all slots.

        if len(votes.from_nodes) >= 2 * self._f + 1:

            if self._follow_view.get_leader() == self._index and not self._is_leader:

                self._log.info("%d: Change to be leader!! view_number: %d", 
                    self._index, self._follow_view.get_view())

                self._is_leader = True
                self._view.set_view(self._follow_view.get_view())
                # TODO: More efficient way to find last slot with prepare certificate.
                last_certificate_slot = max(
                    [int(slot) for slot in votes.prepare_certificate_by_slot] + [-1])

                # Update the next_slot!!
                self._next_propose_slot = last_certificate_slot + 1

                proposal_by_slot = {}
                for i in range(self._ckpt.next_slot, last_certificate_slot + 1):
                    slot = str(i)
                    if slot not in votes.prepare_certificate_by_slot:

                        self._log.debug("%d decide no_op for slot %d", 
                            self._index, int(slot))

                        proposal = {
                            'id': (-1, -1),
                            'client_url': "no_op",
                            'timestamp':"no_op",
                            'data': MessageType.NO_OP
                        }
                        proposal_by_slot[slot] = proposal
                    elif not self._status_by_slot[slot].commit_certificate:
                        proposal = votes.prepare_certificate_by_slot[slot].get_proposal()
                        proposal_by_slot[slot] = proposal

                await self.fill_bubbles(proposal_by_slot)
        return web.Response()

    async def fill_bubbles(self, proposal_by_slot):
        '''
        Fill the bubble during view change. Basically, it's a 
        preprepare that assign the proposed slot instead of using 
        new slot.

        input: 
            proposal_by_slot: dictionary that keyed by slot and 
            the values are the preprepared proposals
        '''
        self._log.info("%d: on fill bubbles.", self._index)
        self._log.debug("Number of bubbles: %d", len(proposal_by_slot))

        bubbles = {
            'leader': self._index,
            'view': self._view.get_view(),
            'proposal': proposal_by_slot,
            'type': 'preprepare'
        }
        
        await self._post(self._nodes, MessageType.PREPARE, bubbles)

    async def garbage_collection(self):
        '''
        Delete those status in self._status_by_slot if its 
        slot smaller than next slot of the checkpoint.
        '''
        await asyncio.sleep(self._sync_interval)
        delete_slots = []
        for slot in self._status_by_slot:
            if int(slot) < self._ckpt.next_slot:
                delete_slots.append(slot)
        for slot in delete_slots:
            del self._status_by_slot[slot]

        # Garbage collection for cjeckpoint.
        await self._ckpt.garbage_collection()


    async def show_blockchain(request):
        name = request.match_info.get( "Anonymous")
        text = "show blockchain here " 
        print('Node '+str(self._index)+' anything')
        return web.Response(text=text)

