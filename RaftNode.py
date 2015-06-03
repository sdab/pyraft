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
        # a global lock for thread synhronization between the poll method
        # and the rpc methods
        self.lock = threading.Lock()

        # init server
        processor = Raft.Processor(self)
        transport = TSocket.TServerSocket(port=port)

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
            self.members = []
            for (addr,port) in self.initialMembers:
                client = connectClient(addr,port)

                self.members.append(client)

    def poll(self):
        with self.lock:
            time.sleep(1)

    def stop(self):
        try:
            self.server.stop()
            self.listen_thread.join()
        except Exception as e:
            print 'exception on stop',e

    # The rpc methods
    def ping(self):
        # doesnt actually need the lock, but for good measure
        with self.lock:
            return 'pong'

    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        # TODO: remove
        print 'called appendEntries'
        # TODO: figure out what to do with leaderId
        with self.lock:
            ret = AppendReturn(self.currentTerm,False)
            try:
                # Reply false if term < currentTerm
                if term < self.currentTerm:
                    # TODO: remove
                    print 'term is too old'
                    return ret

                # Reply false if log doesn't contain an entry at prevLogIndex
                # whose term matches prevLogTerm
                # case for empty log
                if ( len(self.log) > 0 or prevLogIndex >= 0 ) and \
                  (len(self.log) < prevLogIndex+1 or self.log[prevLogIndex][0] != prevLogTerm):
                    # TODO: remove
                    print 'no entry at prevLogIndex'
                    return ret

                # If an existing entry conflicts with a new one (same index
                # but different terms), delete the existing entry and all that
                # follow it
                for i in range(prevLogIndex+1,len(self.log)):
                    entryIndex = i - prevLogIndex - 1
                    if entryIndex > len(entries):
                        break
                    # TODO: should only the term match?
                    if self.log[i] != (term,entries[entryIndex]):
                        self.log = self.log[:i]
                        break

                # Append any new entries not already in the log
                insertIndex = prevLogIndex+1
                for e in entries:
                    if len(self.log) > insertIndex:
                        self.log[insertIndex] = (term,e)
                    else:
                        self.log.append((term,e))

                # If leaderCommit > commitIndex, set commitIndex =
                # min(leaderCommit, index of last new entry)
                if leaderCommit > self.commitIndex:
                    self.commitIndex = min(leaderCommit,len(self.log) - 1)

                # TODO: remove
                print 'log is now',self.log
                ret.success = True

            except Exception as err:
                print err
            return ret

    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        with self.lock:
            pass

    def installSnapshot(self, term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done):
        with self.lock:
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
    nodeB = runNode(8081,[('localhost',8080)])

    # make client
    client = connectClient('localhost',8080)
    print client.ping()

    try:
        while True:
            node.poll()
            nodeB.poll()
    except:
        pass
    finally:
        print 'stopping'
        nodeB.stop()
        node.stop()
