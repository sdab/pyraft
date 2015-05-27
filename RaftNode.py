import socket
import threading
import time

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
        self.halt = False
        
        # init server socket
        self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serversocket.bind(('localhost', port))
        self.serversocket.settimeout(3)
        
        self.initialMembers = initialMembers
        
    def start(self):
        self.serversocket.listen(5)
        # start listening thread
        self.listen_thread = threading.Thread(target=self.listen)
        self.listen_thread.start()

    def listen(self):
        print 'listening'

        while not self.halt:
            try:
                connection, client_address = self.serversocket.accept()

                print 'recieved connection from',client_address
        
                for line in connection.makefile('r'):
                    # TODO: handle line
                    print line[:-1]
                print 'finished with connection from',client_address
            except:
                pass
        print 'done listening'

    def connect(self):
        # init other sockets to connect to other members
        if self.initialMembers != None:
            self.initialMembers = initialMembers
            self.members = []
            for (addr,port) in initialMembers:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((addr,port))
                self.members.append(s)


    def poll(self):
        time.sleep(1)

    def stop(self):
        self.halt = True
        self.serversocket.close()
        self.listen_thread.join()

    # The rpc methods
    def ping(self):
        return 'pong'

    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        pass

    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        pass

    def installSnapshot(self, term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, data, done):
        pass

        
def runNode(port,members=None):
    node = RaftNode(port,members)
    node.start()
    node.connect()

    return node

if __name__ == '__main__':
    node = runNode(8080)

    # make client socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost',8080))
    s.sendall('ping\n')
    s.close()
    try:
        while True:
            node.poll()
    finally:
        node.stop()
        
