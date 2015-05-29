import Queue
import os
import sys
import threading
import traceback

from thrift.server.TServer import TServer
from thrift.Thrift import TProcessor
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

import logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Basically a copy of TThreadPoolServer with a stop() call
# to allow for clean shutdowns. This comes at the cost of having
# timeouts on the socket and Queue which may increase cpu churn.
class ThreadPoolThriftServer(TServer):
  """Server with a fixed size pool of threads which service requests."""

  def __init__(self, *args, **kwargs):
    TServer.__init__(self, *args)
    self.clients = Queue.Queue()
    self.threads = 10
    self.serverThreads = []
    self.daemon = kwargs.get("daemon", False)
    self.isRunning = False
    
    
  def setNumThreads(self, num):
    """Set the number of worker threads that should be created"""
    self.threads = num

  def serveThread(self):
    """Loop around getting clients from the shared queue and process them."""
    while self.isRunning:
      try:
        # TODO: make timeout configurable
        client = self.clients.get(True,3)
        self.serveClient(client)
      except Exception, x:
        logger.exception(x)

  def serveClient(self, client):
    """Process input/output from a client for as long as possible"""
    itrans = self.inputTransportFactory.getTransport(client)
    otrans = self.outputTransportFactory.getTransport(client)
    iprot = self.inputProtocolFactory.getProtocol(itrans)
    oprot = self.outputProtocolFactory.getProtocol(otrans)
    try:
      while self.isRunning:
        self.processor.process(iprot, oprot)
    except TTransport.TTransportException, tx:
      pass
    except Exception, x:
      logger.exception(x)

    itrans.close()
    otrans.close()

  def serve(self):
    """Start a fixed number of worker threads and put client into a queue"""
    self.isRunning = True
    for i in range(self.threads):
      try:
        t = threading.Thread(target=self.serveThread)
        t.setDaemon(self.daemon)
        t.start()
        self.serverThreads.append(t)
      except Exception, x:
        logger.exception(x)

    # Pump the socket for clients
    self.serverTransport.listen()
    # TODO: set the timeout properly in a configurable manner
    if hasattr(self.serverTransport.handle, 'settimeout'):
      self.serverTransport.handle.settimeout(3)
      
    while self.isRunning:
      try:
        client = self.serverTransport.accept()
        if not client:
          continue
        self.clients.put(client)

      except (SystemExit, KeyboardInterrupt):
        logger.exception(x)

      except Exception, x:
        logger.exception(x)

    for t in self.serverThreads:
      t.join()

  def stop(self):
    if not self.isRunning:
      return
    
    self.isRunning = False
