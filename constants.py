class MessageType():
    REQUEST = 'request'
    PREPREPARE = 'preprepare'
    PREPARE = 'prepare'
    COMMIT = 'commit'
    REPLY = 'reply'

    FEEDBACK = "FEEDBACK"
    CONFIRM = "CONFIRM"
    FAST_REPLY = "FAST_REPLY"

    JOIN = 'join'
    JOIN_REPLY = 'join_reply'


    NO_OP = 'NOP'

    RECEIVE_SYNC = 'receive_sync'
    RECEIVE_CKPT_VOTE = 'receive_ckpt_vote'

    VIEW_CHANGE_REQUEST = 'view_change_request'
    VIEW_CHANGE_VOTE = "view_change_vote"