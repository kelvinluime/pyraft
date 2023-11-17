import unittest
from pyraft import raft_server

class TestRaftServer(unittest.TestCase):
    def test_start_server(self):
        server = raft_server.RaftServer(0, [1, 2, 3])
        server.start(8000)

        
if __name__ == '__main__':
    unittest.main()