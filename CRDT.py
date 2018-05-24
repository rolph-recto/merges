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

# state-based counter that uses vector clocks
class StateCounter(CRDT):
  def __init__(self, name):
    super(StateCounter, self).__init__(name)
    self.storeValues = {}
  
  @CRDT.executor("StateCounterInc")
  def _doInc(self, data):
    if data["store_id"] in self.storeValues:
      self.storeValues[data["store_id"]] += data["inc_amt"]
    else:
      self.storeValues[data["store_id"]] = data["inc_amt"]

  @CRDT.command
  def inc(self, amount):
    data = { "store_id": self.store.id, "inc_amt": amount }
    return CRDTCommand("StateCounterInc", data)

  def get(self):
    total_value = 0
    for store_value in self.storeValues.values():
      total_value += store_value

    return total_value

  def reset(self):
    v = self.get()
    self.inc(0 - v)

  def __str__(self):
    return str(self.get())


# use Lamport clocks to establish total ordering on writes
class LWWRegister(CRDT):
  def __init__(self, name):
    super(LWWRegister, self).__init__(name)
    self.t = 0
    self.value = 0

  @CRDT.executor("LWWRegisterSet")
  def _doSet(self, data):
    if data["t"] > self.t:
      self.t = data["t"]
      self.value = data["value"]
    elif data["t"] == self.t and data["store"] > self.store.id:
      self.value = data["value"]
    elif data["store"] == self.store.id:
      self.value = data["value"]

  @CRDT.command
  def set(self, v):
    self.t += 1
    data = { "t": self.t, "store": self.store.id, "value": v }
    return CRDTCommand("LWWRegisterSet", data)

  def get(self):
    return self.value

  def __str__(self):
    return str(self.value)


class ORSet(CRDT):
  def __init__(self, name):
    super(ORSet, self).__init__(name)
    self.adds = set()
    self.tombstones = set()

  @CRDT.executor("ORSetAdd")
  def _doAdd(self, data):
    x = (data["elem"], data["id"])
    self.adds.add(x)

  @CRDT.command
  def add(self, e):
    return CRDTCommand("ORSetAdd", { "elem": e, "id": str(uuid.uuid1()) })

  @CRDT.executor("ORSetRemove")
  def _doRemove(self, data):
    for elem_id in data["ids"]:
      self.tombstones.add((data["elem"], elem_id))

  @CRDT.command
  def remove(self, e):
    ids = []
    for x in self.adds:
      if x[0] == e:
        ids.append(x[1])

    return CRDTCommand("ORSetRemove", { "elem": e, "ids": ids })

  @CRDT.executor("ORSetRemoveAll")
  def _doRemoveAll(self, data):
    for elem in data["elems"]:
      self.tombstones.add(elem)

  @CRDT.command
  def removeAll(self):
    return CRDTCommand("ORSetRemoveAll", { "elems": self.adds })

  def reset(self):
    self.removeAll()

  def contains(self, e):
    for x in self.adds:
      if x[0] == e and x not in self.tombstones:
        return True

    return False

  def getAll(self):
    s = set()
    for x in self.adds:
      if x[0] not in s and x not in self.tombstones:
        s.add(x[0])

    return s

  def __str__(self):
    return str(self.getAll())


# CRDT map whose buckets are also CRDTs
# NOTE: this currently does NOT support concurrent PUTs to different CRDT values
# to support that, we essentially need to treat each bucket as LWW-registers 
# that contain the name of the CRDT in that bucket
class CRDTMap(CRDT):
  def __init__(self, name):
    super(CRDTMap, self).__init__(name)
    self.keyset = ORSet(name + "_keys")
    self.map = {}

  def setStore(self, store):
    self.store = store
    self.store.register(self.keyset)

  @CRDT.executor("CRDTMapPut")
  def _doPut(self, data):
    self.map[data["key"]] = data["value"]

  @CRDT.command
  def put(self, key, crdt):
    self.store.register(crdt)
    self.keyset.add(key)
    return CRDTCommand("CRDTMapPut", { "key": key, "value": crdt.name })

  # remove is not a CRDT command --- i.e. it doesn't generate a CRDTCommand
  # because all the needed commands are generated by the child CRDTs
  # (the keyset and the value CRDT)
  def remove(self, key):
    if self.keyset.contains(key):
      self.keyset.remove(key)
      # trigger reset on value CRDT so in the presence of concurrent updates
      # to it, the intention of the user at the source site is preserved
      # (see the comment in __getitem__)
      # this is interesting because this is a parent CRDT operation
      # propagating *down* to trigger updates to a child CRDT
      self.store[self.map[key]].reset()

  def __setitem__(self, key, crdt):
    self.put(key, crdt)

  # treat an access as a put to have the correct semantics w.r.t. conflicting
  # deletes and updates
  # e.g. consider a map m = { A: Set(1) } and concurrent updates:
  # replica 1: m[A].add(2)
  # replicate 2: m.remove(A)
  # when both replicas quiesce, their maps should be { A: Set(2) }
  # so the access works like a put inserting the same value at the accessed key,
  def __getitem__(self, key):
    if self.keyset.contains(key):
      self.keyset.add(key)
      return self.store[self.map[key]]
    else:
      raise KeyError("Key " + key + " not found in CRDT map")

  def __str__(self):
    m = {}
    for key, value_name in self.map.iteritems():
      if self.keyset.contains(key):
        m[key] = str(self.store[value_name])

    return str(m)


def test1():
  # store 1
  s1 = CRDTStore()

  s1.register(ORSet("set"))
  s1["set"].add(1)
  s1["set"].add(2)
  s1["set"].remove(1)

  s1.register(StateCounter("scounter"))
  s1["scounter"].inc(10)

  # store 2
  s2 = CRDTStore()

  s2.register(ORSet("set"))
  s2["set"].add(1)
  s2["set"].add(3)

  s2.register(StateCounter("scounter"))
  s2["scounter"].inc(-20)
  s2["scounter"].reset()

  s2.connect(s1)
  s2.sync()

  print s1["set"], s2["set"]
  print s1["scounter"], s2["scounter"]


def test2():
  s1 = CRDTStore()
  s1.register(CRDTMap("bib"))
  s1["bib"]["Hemingway"] = ORSet("Hemingway_books")
  s1["bib"]["Hemingway"].add("A Farewell to Arms")

  s2 = CRDTStore()
  s2.connect(s1)
  # unfortunately, we have to do this because the store is not smart enough
  # to create CRDTs when they are created in some other store/replica.
  # it will only sync updates between registered CRDTs with the same ID.
  # supporting dynamic creation of CRDTs for synchronization
  # should be possible though
  s2.register(CRDTMap("bib"))
  s2.register(ORSet("Hemingway_books"))

  s2.sync()

  # concurrent updates here
  s1["bib"]["Hemingway"].add("The Sun Also Rises")
  s2["bib"].remove("Hemingway")

  s2.sync()

  # expected: { "Hemingway": set("The Sun Also Rises") }
  print s1["bib"], s2["bib"]


def main():
  test2()

if __name__ == "__main__":
  main()


