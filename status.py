from asyncio.events import set_event_loop
import hashlib
import json

from constants import MessageType
class Status:
    '''
    Record the state for every slot.
    '''

    def __init__(self, f):
        self.f = f
        self.request = 0
        self.prepare_msgs = {}     
        self.prepare_certificate = None # proposal
        self.commit_msgs = {}
        # Only means receive more than 2f + 1 commit message,
        # but can not commit if there are any bubbles previously.
        self.commit_certificate = None # proposal

        # {(view, digest(proposal)) : SequenceElement}
        self.feedback_msgs = {}
        self.feedback_certificate = None
    
        # Set it to True only after commit
        self.is_committed = False

        self.join_msgs = {}
        self.is_joined = False
        self.join_accept_msgs = {}
        self.is_join_accepted = False
        self.join_reply_msgs = {}
        self.get_enough_join_reply = False
    
    class Certificate:
        def __init__(self, view, proposal = 0):
            '''
            input:
                view: object of class View
                proposal: proposal in json_data(dict)
            '''
            self._view = view
            self._proposal = proposal

        def to_dict(self):
            '''
            Convert the Certificate to dictionary
            '''
            return {
                'view': self._view.get_view(),
                'proposal': self._proposal
            }

        def dumps_from_dict(self, dictionary):
            '''
            Update the view from the form after self.to_dict
            input:
                dictionay = {
                    'view': self._view.get_view(),
                    'proposal': self._proposal
                }
            '''
            self._view.set_view(dictionary['view'])
            self._proposal = dictionary['proposal']
        def get_proposal(self):
            return self._proposal


    class SequenceElement:
        def __init__(self, proposal):
            self.proposal = proposal
            self.from_nodes = set([])

    def _update_sequence(self, msg_type, view, proposal, from_node):
        '''
        Update the record in the status by message type
        input:
            msg_type: Status.PREPARE or Status.COMMIT
            view: View object of self._follow_view
            proposal: proposal in json_data
            from_node: The node send given the message.
        '''

        # The key need to include hash(proposal) in case get different 
        # preposals from BFT nodes. Need sort key in json.dumps to make 
        # sure getting the same string. Use hashlib so that we got same 
        # hash everytime.
        hash_object = hashlib.sha256(json.dumps(proposal, sort_keys=True).encode())
        key = (view.get_view(), hash_object.digest())
        if msg_type == MessageType.PREPARE:
            if key not in self.prepare_msgs:
                self.prepare_msgs[key] = self.SequenceElement(proposal)
            self.prepare_msgs[key].from_nodes.add(from_node)
        elif msg_type == MessageType.COMMIT:
            if key not in self.commit_msgs:
                self.commit_msgs[key] = self.SequenceElement(proposal)
            self.commit_msgs[key].from_nodes.add(from_node)
        elif msg_type == MessageType.FEEDBACK:
            if key not in self.feedback_msgs:
                self.feedback_msgs[key] = self.SequenceElement(proposal)
            self.feedback_msgs[key].from_nodes.add(from_node)
        elif msg_type == MessageType.JOIN:
            if key not in self.join_msgs:
                self.join_msgs[key] = self.SequenceElement(proposal)
            self.join_msgs[key].from_nodes.add(from_node)
        elif msg_type == MessageType.JOIN_ACCEPT:
            if key not in self.join_accept_msgs:
                self.join_accept_msgs[key] = self.SequenceElement(proposal)
            self.join_accept_msgs[key].from_nodes.add(from_node)
        elif msg_type == MessageType.JOIN_REPLY:
            if key not in self.join_reply_msgs:
                self.join_reply_msgs[key] = self.SequenceElement(proposal)
            self.join_reply_msgs[key].from_nodes.add(from_node)

    def _check_majority(self, msg_type):
        '''
        Check if receive more than 2f + 1 given type message in the same view.
        input:
            msg_type: self.PREPARE or self.COMMIT
        '''
        if msg_type == MessageType.PREPARE:
            if self.prepare_certificate:
                return True
            for key in self.prepare_msgs:
                if len(self.prepare_msgs[key].from_nodes)>= 2 * self.f + 1:
                    return True
            return False

        elif msg_type == MessageType.COMMIT:
            if self.commit_certificate:
                return True
            for key in self.commit_msgs:
                if len(self.commit_msgs[key].from_nodes) >= 2 * self.f + 1:
                    return True
            return False 

        # must receive 3f + 1 feedback instead of 2f + 1 
        elif msg_type == MessageType.FEEDBACK:
            if self.feedback_certificate:
                return True
            for key in self.feedback_msgs:
                if len(self.feedback_msgs[key].from_nodes) == 3 * self.f + 1:
                    return True
            return False 

        elif msg_type == MessageType.JOIN:
            if self.is_joined:
                return True
            for key in self.join_msgs:
                if len(self.join_msgs[key].from_nodes) >= 2 * self.f + 1:
                    self.is_joined = True
                    return True
            return False 

        elif msg_type == MessageType.JOIN_ACCEPT:
            if self.is_join_accepted:
                return True
            for key in self.join_accept_msgs:
                if len(self.join_accept_msgs[key].from_nodes) >= 2 * self.f + 1:
                    self.is_join_accepted = True
                    return True
            return False 

        elif msg_type == MessageType.JOIN_REPLY:
            if self.get_enough_join_reply:
                return True
            for key in self.join_reply_msgs:
                if len(self.join_reply_msgs[key].from_nodes) >= self.f + 1:
                    self.get_enough_join_reply = True
                    return True
            return False 
