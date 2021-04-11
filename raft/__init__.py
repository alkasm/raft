import enum
import logging
import typing


class Role(enum.Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class VoteRequest(typing.NamedTuple):
    node_id: int
    current_term: int
    log_length: int
    last_term: int


class VoteResponse(typing.NamedTuple):
    node_id: int
    current_term: int
    yea: bool


class LogRequest(typing.NamedTuple):
    leader_id: int
    term: int
    ind: int
    prev_log_term: int
    commit_length: int
    entries: typing.List["LogEntry"]


class LogResponse(typing.NamedTuple):
    node_id: int
    term: int
    ack_length: int
    log_ok: bool


class BroadcastMessage(typing.NamedTuple):
    data: typing.Any


RaftMessage = typing.Union[VoteRequest, VoteResponse, LogRequest, LogResponse]


class LogEntry(typing.NamedTuple):
    message: BroadcastMessage
    term: int  # term of the leader at the time of broadcast


class Node:
    def __init__(self, node_id):
        # must be persisted
        self.term = 0
        self.voted_for = None
        self._log: typing.List[LogEntry] = []
        self.commit_length = 0
        # maybe persisted?
        self.node_id = node_id
        # can be ephemeral
        self.role = Role.FOLLOWER
        self.leader = None
        self.votes_received = set()
        self.sent_length = {}
        self.acked_length = {}
        # election timer

    @property
    def log(self):
        logging.debug(
            "Accessing log on node %d with entries: %r", self.node_id, self._log
        )
        return self._log

    def promote(self):
        # Node suspects leader has failed, or election has timed out
        self.term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        last_term = self.log[-1].term if self.log else 0
        req = VoteRequest(
            self.node_id,
            self.term,
            len(self.log),
            last_term,
        )
        send_all(req, self.node_id)
        # start election timer

    def recv_vote_request(self, msg: VoteRequest):
        candidate_id, candidate_term, candidate_log_length, candidate_log_term = msg

        # ensure the candidate's log isn't behind our own
        log_term = self.log[-1].term if self.log else 0
        log_ok = (candidate_log_term > log_term) or (
            candidate_log_term == log_term and candidate_log_length >= len(self.log)
        )

        # ensure we only vote once per term
        term_ok = (candidate_term > self.term) or (
            candidate_term == self.term and self.voted_for in {candidate_id, None}
        )

        yea = log_ok and term_ok
        if yea:
            self.term = candidate_term
            self.role = Role.FOLLOWER
            self.voted_for = {candidate_id}
        resp = VoteResponse(self.node_id, self.term, yea)
        send(resp, candidate_id)

    def recv_vote_response(self, msg: VoteResponse):
        voter_id, voter_term, yea = msg
        if self.role is Role.CANDIDATE and voter_term == self.term and yea:
            # successful vote
            self.votes_received.add(voter_id)
            if quorum(len(self.votes_received)):
                self.role = Role.LEADER
                self.leader = self.node_id
                # cancel_election_timer()
            for follower in nodes.keys() - {self.node_id}:
                self.sent_length[follower] = len(self.log)
                self.acked_length[follower] = 0
                self.replicate_log(follower)
        elif voter_term > self.term:
            self.term = voter_term
            self.role = Role.FOLLOWER
            self.voted_for = None
            # cancel election timer

    def broadcast(self, msg: BroadcastMessage):
        # only the leader should be broadcasting messages
        if self.role is Role.LEADER:
            self.log.append(LogEntry(msg, self.term))
            self.acked_length[self.node_id] = len(self.log)
            for follower in nodes.keys() - {self.node_id}:
                self.replicate_log(follower)
        else:
            nodes[self.leader].broadcast(msg)

    # periodically replicate log across all nodes if you are the leader

    def replicate_log(self, follower_id: int):
        ind = self.sent_length[follower_id]
        entries = self.log[ind:]
        prev_log_term = self.log[ind - 1].term if ind > 0 else 0
        req = LogRequest(
            self.node_id, self.term, ind, prev_log_term, self.commit_length, entries
        )
        send(req, follower_id)

    def recv_log_request(self, msg: LogRequest):
        leader_id, term, log_length, log_term, leader_commit, entries = msg
        if term > self.term:
            self.term = term
            self.voted_for = None

        # log_length does NOT include the new entries
        # check that we are at least up to date with the old entries
        log_ok = len(self.log) >= log_length
        if log_ok and log_length > 0:
            # if the terms agree, then RAFT guarantees the logs are consistent up to log_length
            log_ok = log_term == self.log[log_length - 1].term

        ack = 0
        log_ok = log_ok and term == self.term
        if log_ok:
            self.role = Role.FOLLOWER
            self.leader = leader_id
            self.append_entries(log_length, leader_commit, entries)
            ack = log_length + len(entries)
        resp = LogResponse(self.node_id, self.term, ack, log_ok)
        send(resp, leader_id)

    def append_entries(self, log_length, leader_commit, entries):
        if entries and len(self.log) > log_length:
            # if the logs are inconsistent, discard the entries
            if self.log[log_length].term != entries[0].term:
                self.log = self.log[:log_length]
        if log_length + len(entries) > len(self.log):
            self.log.extend(entries[len(self.log) - log_length :])

        # deliver committed messages to the application
        for entry in self.log[self.commit_length : leader_commit]:
            self.deliver(entry.message)
        self.commit_length = max(self.commit_length, leader_commit)

    def deliver(self, msg: BroadcastMessage):
        logging.info("Node %s received message %r!", self.node_id, msg)

    def recv_log_response(self, msg: LogResponse):
        follower_id, term, ack, success = msg
        if term == self.term and self.role is Role.LEADER:
            if success:
                self.sent_length[follower_id] = ack
                self.acked_length[follower_id] = ack
                self.commit_log_entries()
            elif self.sent_length[follower_id] > 0:
                # if the logs are inconsistent, send more entries
                self.sent_length[follower_id] -= 1
                self.replicate_log(follower_id)
        elif term > self.term:
            self.term = term
            self.role = Role.FOLLOWER
            self.voted_for = None

    def acks(self, ack_length: int):
        # how many nodes have acknowledged at least ack_length entries?
        return len({n for n in nodes if self.acked_length[n] >= ack_length})

    def commit_log_entries(self):
        ready = next(
            filter(lambda l: quorum(self.acks(l)), range(len(self.log), 1, -1)), 0
        )
        if ready > self.commit_length and self.log[ready - 1].term == self.term:
            # cannot commit a previous leader's log entries; leader must add at
            # least one entry to the log before it can commit
            for entry in self.log[self.commit_length : ready]:
                self.deliver(entry.message)
            self.commit_length = ready

    def recv(self, msg: RaftMessage):
        if isinstance(msg, VoteRequest):
            self.recv_vote_request(msg)
        elif isinstance(msg, VoteResponse):
            self.recv_vote_response(msg)
        elif isinstance(msg, LogRequest):
            self.recv_log_request(msg)
        elif isinstance(msg, LogResponse):
            self.recv_log_response(msg)
        else:
            logging.error("Unknown message type %r", type(msg))


def quorum(n):
    return n >= len(nodes) // 2 + 1


def send(msg: RaftMessage, node_id: int):
    nodes[node_id].recv(msg)


def send_all(msg: RaftMessage, from_node_id: int):
    for node_id in nodes.keys() - {from_node_id}:
        nodes[node_id].recv(msg)


nodes = {}
