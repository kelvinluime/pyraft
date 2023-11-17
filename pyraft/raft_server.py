import xmlrpc.client
import random
import time
import pickle
import os
from enum import Enum
from xmlrpc.server import SimpleXMLRPCServer
from typing import List
from threading import Thread

class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class LogEntry:
    def __init__(self, term: int, command: str):
        self.term = term
        self.command = command

class _PersistentState:
    def __init__(self, raft_config_file: str):
        self.raft_config_file = raft_config_file

    def _load_config_file(self):
        with open(self.raft_config_file, 'rb') as f:
            return pickle.load(f)
        
    def _save_config_file(self, config):
        with open(self.raft_config_file, 'wb') as f:
            pickle.dump(config, f)

    def get_current_term(self) -> int:
        return self._load_config_file()['current_term']
    
    def set_current_term(self, term: int):
        config = self._load_config_file()
        config['current_term'] = term
        self._save_config_file(config)

    def get_voted_for(self) -> int:
        return self._load_config_file()['voted_for']
    
    def set_voted_for(self, voted_for: int):
        config = self._load_config_file()
        config['voted_for'] = voted_for
        self._save_config_file(config)

    def get_log(self) -> List[LogEntry]:
        return self._load_config_file()['log']
    
    def set_log(self, log: List[LogEntry]):
        config = self._load_config_file()
        config['log'] = log
        self._save_config_file(config)


class _RaftServerHeartbeatThread(Thread):
    def __init__(self, server: 'RaftServer'):
        super().__init__()
        self.server = server
        self.last_leader_contact = 0

    def run(self):
        while True:
            if self.server.state != RaftState.LEADER:
                return
            
            for server_id in self.server.peers:
                if server_id == self.server.id:
                    continue

class RaftServer:
    ELECTION_TIMEOUT_RANGE_MS = (150, 300)

    def __init__(self, id: int, peers: List):
        """
        Initialize the Raft server.

        args:
            id (int): the server's id
            peers (list): a list of peer ids

        returns:
            None
        """
        self._id = id
        self._peers = peers

        # Init persistent state
        home_dir = os.path.expanduser("~")
        self._persistent_state = _PersistentState(f"{home_dir}/.pyraft/{id}/config.pkl")

        # Volatile state
        self._commit_index = 0
        self._last_applied = 0
        self._next_index = []
        self._match_index = []

        self._server_state = RaftState.FOLLOWER


    def _append_entries_rpc(self, term: int, leader_id: int, prev_log_index, prev_log_term, entries, leader_commit) -> (int, bool):
        """
        Invoked by leader to replicate log entries; also used as heartbeat.

        args:
            term (int): leader's term
            leader_id (int): so follower can redirect clients
            prev_log_index (int): index of log entry immediately preceding new ones
            prev_log_term (int): term of prev_log_index entry
            entries (list): log entries to store (empty for heartbeat; may send more than one for efficiency)
            leader_commit (int): leader's commit_index
        
        returns:
            (bool, int): success, current_term
        """
        current_term = self._persistent_state.get_current_term()
        log = self._persistent_state.get_log()

        if term < current_term:
            return (False, current_term)
        
        if len(self._log) - 1 < prev_log_index or self._log[prev_log_index].term != prev_log_term:
            return (False, current_term)
        
        self._log = self._log[:prev_log_index + 1] + entries
        self._commit_index = min(leader_commit, len(self._log) - 1)

        return (True, self._current_term)


    def _request_vote_rpc(self, term: int, candidate_id: int, last_log_index: int, last_log_term: int) -> (int, bool):
        """
        Invoked by candidates to gather votes.

        args:
            term (int): candidate's term
            candidate_id (int): candidate requesting vote
            last_log_index (int): index of candidate's last log entry
            last_log_term (int): term of candidate's last log entry

        returns:
            (bool, int): vote_granted, term
        """
        if term < self._current_term:
            return (self._current_term, False)
        
        if self._voted_for is None or self._voted_for == candidate_id:
            if last_log_index >= len(self._log) - 1:
                return (self._current_term, True)
            
        return (self._current_term, False)
    

    def _install_snapshot_rpc(self, term: int, leader_id: str, last_included_index: int, last_included_term: int, offset: int, data: List[str], done: bool) -> int:
        """
        Invoked by leader to send chunks of a snapshot to a follower. 
        Leaders always send chunks in order.

        args:
            term (int): leader's term
            leader_id (int): so follower can redirect clients
            last_included_index (int): the snapshot replaces all entries up through and including this index
            last_included_term (int): term of last_included_index
            offset (int): byte offset where chunk is positioned in the snapshot file
            data (list): raw bytes of the snapshot chunk, starting at offset
            done (bool): true if this is the last chunk

        returns:
            int: current_term, for leader to update itself
        """
        if term < self._current_term:
            return self._current_term
        
        return 0


    def start(self, port: int):
        """
        Start the server's event loop.
        """
        server = SimpleXMLRPCServer(("localhost", port))
        print(f"Listening on port {port}...")
        server.register_function(self._append_entries_rpc, "append_entries_rpc")
        server.register_function(self._request_vote_rpc, "request_vote_rpc")
        server.serve_forever()
