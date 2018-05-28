#!/usr/bin/env python
# a toy implementation of CRDTs

# compare this implementation with actual CRDT implementations:
# https://docs.basho.com/riak/kv/2.1.1/developing/data-types/

import uuid
import random

class CRDTCommand(object):
  def __init__(self, cmdtype, data):
    self.source = None
    self.type = cmdtype
    self.data = data

  def setSource(self, source):
    self.source = source

  def __str__(self):
    return "Command: " + self.type +"; Data: " + str(self.data)


class CRDTStore(object):
  # this is used to establish arbitration order for some of the CRDTs
  store_id = 0

  def __init__(self):
    self.id = CRDTStore.store_id
    self.counter = 0
    CRDTStore.store_id += 1
    self.connections = set()
    self.crdts = {}
    self.msgs = []

  def __getitem__(self, crdt):
    return self.crdts[crdt]

  def register(self, crdt):
    crdt.setStore(self)
    self.crdts[crdt.name] = crdt

  def addStore(self, store):
    self.connections.add(store)

  def connect(self, store):
    self.addStore(store)
    store.addStore(self)

  # don't publish immediately; add to list of in-flight messages
  def publish(self, cmd):
    self.msgs.append(cmd)

  def receive(self, cmd):
    if cmd.source in self.crdts:
      self.crdts[cmd.source].executeCommand(cmd)

  def broadcast(self):
    # to simulate network weirdness, shuffle messages
    random.shuffle(self.msgs)

    for msg in self.msgs:
      for store in self.connections:
        store.receive(msg)

    self.msgs = []

  # send in-flight messages to other stores
  def sync(self):
    self.broadcast()

    for store in self.connections:
      store.broadcast()

  def next_dot(self):
    self.counter += 1
    return (self.id, self.counter)


class CRDT(object):
  executorMap = {}

  def __init__(self, name):
    self.store = None
    self.name = name

  # set CRDT to bottom; this only makes sense for some CRDTs
  # (e.g. OR sets but not Grow-Only sets)
  def reset(self):
    raise Exception("This CRDT does not support resets")

  def setStore(self, store):
    self.store = store

  def publish(self, cmd):
    self.store.publish(cmd)

  def executeCommand(self, cmd):
    CRDT.executorMap[cmd.type](self, cmd.data)

  # decorator to execute command at the source site
  # and publish the command to the other replicas
  @staticmethod
  def command(f):
    def wrapper(self, *args):
      cmd = f(self, *args)
      cmd.setSource(self.name)
      self.executeCommand(cmd)
      self.publish(cmd)

    return wrapper

  @staticmethod
  def executor(cmdtype):
    def decorator(f):
      CRDT.executorMap[cmdtype] = f
      return f

    return decorator

