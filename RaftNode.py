import socket
import threading
import time

import sys
sys.path.append('gen-py')

from ThreadPoolThriftServer import *

from raft import Raft
from raft.ttypes import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

# an enum
class Role:
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode:

    def __init__(self,port,initialMembers=None):
        # persistent state
        # TODO: make persistent
        self.currentTerm = 0
        self.votedFor = None
        self.log = []

        # volatile state
        self.commitIndex = 0
        self.lastApplied = 0

        # other state
        self.role = Role.FOLLOWER
        self.port = port
        
        # init server
        processor = Raft.Processor(self)
        transport = TSocket.TServerSocket(port=port)
        #transport.setTimeout(1000) # 1s timeout

        #self.server = TServer.TSimpleServer(processor, transport)
        #self.server = TServer.TThreadPoolServer(processor, transport)
        self.server = ThreadPoolThriftServer(processor, transport)

        self.initialMembers = initialMembers
        
    def start(self):
        # start listening thread
        self.listen_thread = threading.Thread(target=self._listen)
        #self.listen_thread.setDaemon(True)
        self.listen_thread.start()

    def _listen(self):
        print 'listening'
        try:
            self.server.serve()
        except Exception as e:
            print '_listen caught',e
        print 'done listening'

    def connect(self):
        # init other sockets to connect to other members
        if self.initialMembers != None:
            self.initialMembers = initialMembers
            self.members = []
            for (addr,port) in initialMembers:
                client = connectClient(addr,port)

                self.members.append(client)

    def poll(self):
        time.sleep(1)

    def stop(self):
        try:
            self.server.stop()
            self.listen_thread.join()
        except Exception as e:
            print 'exception on stop',e

    # The rpc methods
    def ping(self):
        return 'pong'

    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        pass

    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        pass

    def installSnapshot(self, term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done):
        pass

def connectClient(addr,port):
    # Make socket
    transport = TSocket.TSocket(addr, port)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = Raft.Client(protocol)
    
    # Connect
    transport.open()
    return client

        
def runNode(port,members=None):
    node = RaftNode(port,members)
    node.start()
    node.connect()

    return node

if __name__ == '__main__':
    node = runNode(8080)

    # make client
    client = connectClient('localhost',8080)
    print client.ping()
    
    try:
        while True:
            node.poll()
    except:
        pass
    finally:
        print 'stopping'
        node.stop()
        
