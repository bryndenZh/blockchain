import logging
import time

from status import Status

VIEW_SET_INTERVAL = 10

class View:
    def __init__(self, view_number, num_nodes):
        self._view_number = view_number
        self._num_nodes = num_nodes
        self._leader = view_number % num_nodes
        # Minimum interval to set the view number
        self._min_set_interval = VIEW_SET_INTERVAL
        self._last_set_time = time.time()

    # To encode to json
    def get_view(self):
        return self._view_number 

    # Recover from json data.
    def set_view(self, view):
        '''
        Retrun True if successfully update view number
        return False otherwise.
        '''
        if time.time() - self._last_set_time < self._min_set_interval:
            return False
        self._last_set_time = time.time()
        self._view_number = view
        self._leader = view % self._num_nodes
        return True

    def get_leader(self):
        return self._leader

class ViewChangeVotes:
    """
    Record which nodes vote for the proposed view change. 
    In addition, store all the information including:
    (1)checkpoints who has the largest information(largest 
    next_slot) (2) prepare certificate with largest for each 
    slot sent from voted nodes.
    """
    def __init__(self, node_index, num_total_nodes):
        # Current node index.
        self._node_index = node_index
        # Total number of node in the system.
        self._num_total_nodes = num_total_nodes
        # Number of faults tolerand
        self._f = (self._num_total_nodes - 1) // 3
        # Record the which nodes vote for current view.
        self.from_nodes = set([])
        # The prepare_certificate with highest view for each slot
        self.prepare_certificate_by_slot = {}
        self.lastest_checkpoint = None
        self._log = logging.getLogger(__name__)

    def receive_vote(self, json_data):
        '''
        Receive the vote message and make the update:
        (1) Update the inforamtion in given vote storage - 
        prepare certificate.(2) update the node in from_nodes.
        input: 
            json_data: the json_data received by view change vote broadcast:
                {
                    "index": self._index,
                    "view_number": self._follow_view.get_view(),
                    "checkpoint":self._ckpt.get_ckpt_info(),
                    "prepare_certificates":self.get_prepare_certificates(),
                }
        '''
        update_view = None

        prepare_certificates = json_data["prepare_certificates"]

        self._log.debug("%d update prepare_certificate for view %d. %s", 
            self._node_index, json_data['view_number'], str(prepare_certificates))

        for slot in prepare_certificates:
            prepare_certificate = Status.Certificate(View(0, self._num_total_nodes))
            prepare_certificate.dumps_from_dict(prepare_certificates[slot])
            # Keep the prepare certificate who has the largest view number
            if slot not in self.prepare_certificate_by_slot or (
                    self.prepare_certificate_by_slot[slot]._view.get_view() < (
                    prepare_certificate._view.get_view())):
                self.prepare_certificate_by_slot[slot] = prepare_certificate

        self.from_nodes.add(json_data['index'])



