#!/usr/bin/env python
# DeltaCRDT.py
# From Delta State Replicated Datatypes, Almeida et al 2016

from CRDT import *

class DotStore(object):
  def is_bottom(self):
    raise Exception("is_bottom() must be implemented")

  def dots(self):
    raise Exception("dots() must be implemented")

class DotSet(DotStore):
  def __init__(self, s=None):
    self.set = set() if s is None else s

  def is_bottom(self):
    return len(self.set) == 0

  def dots(self):
    return self.set

  def copy(self):
    return DotSet(self.set.copy())

  def __str__(self):
    return "DotSet: " + str(self.set)


# map from dots to values
class DotFun(DotStore):
  def __init__(self, m=None):
    self.map = {} if m is None else m

  def is_bottom(self):
    return len(self.map) == 0

  def dots(self):
    return set(self.map.keys())

  def __setitem__(self, dot, val):
    self.map[dot] = val

  def __getitem__(self, dot):
    return self.map[dot]

  def copy(self):
    return DotFun(self.map.copy())
  
  def __str__(self):
    return "DotFun: " + str(self.map)


# map from arbitrary keys to dot stores
class DotMap(DotStore):
  def __init__(self, val_bottom, m=None):
    # val_bottom is the bottom of the value lattice; this is used for joining
    # when keys aren't mapped to anything
    self.val_bottom = val_bottom
    self.map = {} if m is None else m

  def dots(self):
    d = set()
    for v in self.map.values():
      d |= v.dots()

    return d

  def is_bottom(self):
    return len(self.map) == 0

  def domain(self):
    return set(self.map.keys())

  def __setitem__(self, dot, val):
    self.map[dot] = val

  def __getitem__(self, dot):
    return self.map[dot]

  def copy(self):
    return DotMap(self.val_bottom.copy(), self.map.copy())
  
  def __str__(self):
    strmap = {}
    for k, v in self.map.iteritems():
      strmap[str(k)] = str(v)

    return str(strmap)

    
class CausalCRDT(CRDT):
  def __init__(self, name, ds):
    super(CausalCRDT, self).__init__(name)
    self.dot_store = ds
    self.causal_ctx = set()

  def copy(self, c=None):
    clone = CausalCRDT("", None) if c is None else c
    clone.dot_store = self.dot_store.copy()
    clone.causal_ctx = self.causal_ctx.copy()
    return clone

  def join(self, ccrdt):
    if type(self.dot_store) is DotSet:
      m = (ccrdt.dot_store.set & self.dot_store.set) \
        | (self.dot_store.set - ccrdt.causal_ctx) \
        | (ccrdt.dot_store.set - self.causal_ctx)
      self.dot_store = DotSet(m)
      self.causal_ctx |= ccrdt.causal_ctx

    elif type(self.dot_store) is DotFun:
      m = {}
      for d, v in self.dot_store.map.iteritems():
        if d not in ccrdt.causal_ctx:
          m[d] = v

      for d2, v2 in ccrdt.dot_store.map.iteritems():
        if d2 not in self.causal_ctx:
          m[d2] = v2

      # technically values must be drawn from a lattice and we should join them
      # but in practice this intersection should never happen, because it means
      # a single operation somehow set the register to two values
      # instead, we're just going to arbitrarily draw from the first store
      intersect_keys = self.dot_store.dots() & ccrdt.dot_store.dots()
      for key in intersect_keys:
        m[key] = self.dot_store[key]

      self.dot_store = DotFun(m)
      self.causal_ctx |= ccrdt.causal_ctx

    elif type(self.dot_store) is DotMap:
      union_keys = self.dot_store.domain() | ccrdt.dot_store.domain()
      m = {}
      for key in union_keys:
        ds1 = self.dot_store.map.get(key, self.dot_store.val_bottom).copy()
        ds2 = ccrdt.dot_store.map.get(key, ccrdt.dot_store.val_bottom).copy()
        v1 = CausalCRDT("", ds1)
        v1.causal_ctx = self.causal_ctx.copy()
        v2 = CausalCRDT("", ds2)
        v2.causal_ctx = ccrdt.causal_ctx.copy()
        v1.join(v2)

        if not v1.dot_store.is_bottom():
          m[key] = v1.dot_store

      self.dot_store = DotMap(self.dot_store.val_bottom.copy(), m)
      self.causal_ctx |= ccrdt.causal_ctx

  def __str__(self):
    return "CausalCRDT\n" + str(self.dot_store) + "\nCausal context: " + str(self.causal_ctx) + "\n"


class MVRegister(CausalCRDT):
  def __init__(self, name=""):
    super(MVRegister, self).__init__(name, DotFun())

  def copy(self):
    return super(MVRegister, self).copy(MVRegister())

  # unlike op-based CRDTs, there is a single executor, which just joins
  # the delta from the operation to the current state of the CRDT.
  @CRDT.executor("MVRegister")
  def join(self, data):
    super(MVRegister, self).join(data["delta"])

  @CRDT.command
  def write(self, val):
    dot = self.store.next_dot()
    delta = CausalCRDT("", DotFun())
    delta.dot_store[dot] = val
    delta.causal_ctx = self.dot_store.dots() | set([dot])
    return CRDTCommand("MVRegister", { "delta": delta })

  @CRDT.command
  def clear(self):
    delta = CausalCRDT("", DotFun())
    delta.dot_store = DotFun()
    delta.causal_ctx = self.dot_store.dots()
    return CRDTCommand("MVRegister", { "delta": delta })

  def read(self):
    return self.dot_store.map.values()

  def __str__(self):
    return str(self.read())


class AWSet(CausalCRDT):
  def __init__(self, name=""):
    super(AWSet, self).__init__(name, DotMap(DotSet()))

  def copy(self):
    return super(AWSet, self).copy(AWSet())

  # unlike op-based CRDTs, there is a single executor, which just joins
  # the delta from the operation to the current state of the CRDT.
  @CRDT.executor("AWSet")
  def join(self, data):
    super(AWSet, self).join(data["delta"])

  @CRDT.command
  def add(self, e):
    singdot = set([self.store.next_dot()])
    delta = CausalCRDT("", DotMap(DotSet()))
    delta.dot_store[e] = DotSet(singdot)
    delta.causal_ctx = self.dot_store.map[e].set if e in self.dot_store.map else set()
    delta.causal_ctx |= singdot
    return CRDTCommand("AWSet", { "delta": delta })
  
  @CRDT.command
  def remove(self, e):
    delta = CausalCRDT("", DotMap(DotSet()))
    delta.causal_ctx = self.dot_store.map[e].set if e in self.dot_store.map else set()
    return CRDTCommand("AWSet", { "delta": delta })

  @CRDT.command
  def clear(self):
    delta = CausalCRDT("", DotMap(DotSet()))
    delta.causal_ctx = self.dot_store.dots()
    return CRDTCommand("AWSet", { "delta": delta })

  def elems(self):
    return set(self.dot_store.map.keys())

  def contains(self, e):
    return e in self.elems()

  def __str__(self):
    return str(self.elems())


class RWSet(CausalCRDT):
  def __init__(self, name=""):
    super(RWSet, self).__init__(name, DotMap(DotMap(DotSet())))

  def copy(self):
    return super(RWSet, self).copy(RWSet())
  
  # unlike op-based CRDTs, there is a single executor, which just joins
  # the delta from the operation to the current state of the CRDT.
  @CRDT.executor("RWSet")
  def join(self, data):
    super(RWSet, self).join(data["delta"])

  @CRDT.command
  def add(self, e):
    singdot = set([self.store.next_dot()])
    delta = CausalCRDT("", DotMap(DotMap(DotSet())))
    delta.dot_store[e] = DotMap(DotSet(), { True: DotSet(singdot) })
    delta.causal_ctx = self.dot_store.map[e].dots() if e in self.dot_store.map else set()
    delta.causal_ctx |= singdot
    return CRDTCommand("RWSet", { "delta": delta })

  @CRDT.command
  def remove(self, e):
    singdot = set([self.store.next_dot()])
    delta = CausalCRDT("", DotMap(DotMap(DotSet())))
    delta.dot_store[e] = DotMap(DotSet(), { False: DotSet(singdot) })
    delta.causal_ctx = self.dot_store.map[e].dots() if e in self.dot_store.map else set()
    delta.causal_ctx |= singdot
    return CRDTCommand("RWSet", { "delta": delta })

  @CRDT.command
  def clear(self):
    delta = CausalCRDT("", DotMap(DotMap(DotSet())))
    delta.causal_ctx = self.dot_store.dots()
    return CRDTCommand("RWSet", { "delta": delta })

  def elems(self):
    elems = set()
    for key in self.dot_store.map.keys():
      if False not in self.dot_store.map[key].map.keys():
        elems.add(key)

    return elems

  def __str__(self):
    return str(self.elems())


class ORMap(CausalCRDT):
  def __init__(self, val_bottom, name=""):
    super(ORMap, self).__init__(name, DotMap(val_bottom.dot_store))
    self.val_bottom = val_bottom

  def copy(self):
    return super(ORMap, self).copy(ORMap())

  @CRDT.executor("ORMap")
  def join(self, data):
    super(ORMap, self).join(data["delta"])

  @CRDT.command
  def apply(self, k, f):
    vdots = self.dot_store.map[k].copy() \
        if k in self.dot_store.map \
        else self.dot_store.val_bottom.copy()
    v = self.val_bottom.copy()
    v.store = self.store
    v.published = False
    v.dot_store = vdots
    v.causal_ctx = self.causal_ctx.copy()
    f(v)
    m = { k: v.dot_store }
    delta = CausalCRDT("", DotMap(self.dot_store.val_bottom.copy(), m))
    delta.causal_ctx = v.causal_ctx
    return CRDTCommand("ORMap", { "delta": delta })

  @CRDT.command
  def remove(self, e):
    delta = CausalCRDT("", DotMap(self.val_bottom.dot_store))
    delta.causal_ctx = self.dot_store.map[e].dots() \
        if e in self.dot_store.map else set()
    return CRDTCommand("ORMap", { "delta": delta })

  @CRDT.command
  def clear(self):
    delta = CausalCRDT("", DotMap(DotSet()))
    delta.causal_ctx = self.dot_store.dots()
    return CRDTCommand("ORMap", { "delta": delta })

  def __str__(self):
    strmap = {}
    for k, vdots in self.dot_store.map.iteritems():
      v = self.val_bottom.copy()
      v.dot_store = vdots
      strmap[str(k)] = str(v)

    return str(strmap)


def test1():
  s1 = CRDTStore()
  s1.register(RWSet("set"))
  s1["set"].add("Hemingway")

  s2 = CRDTStore()
  s1.connect(s2)
  s2.register(RWSet("set"))
  s2["set"].add("Kafka")

  s1.sync()

  s1["set"].remove("Hemingway")
  s2["set"].add("Hemingway")

  s1.sync()

  print s1["set"].elems(), s2["set"].elems()


def test2():
  s1 = CRDTStore()
  s1.register(ORMap(AWSet(), "map"))
  s1["map"].apply("Hemingway", lambda v: v.add("A Farewell to Arms"))
  s1["map"].apply("Hemingway", lambda v: v.add("The Sun Also Rises"))

  s2 = CRDTStore()
  s1.connect(s2)
  s2.register(ORMap(AWSet(), "map"))

  s1.sync()

  s1["map"].remove("Hemingway")
  s2["map"].apply("Hemingway", lambda v: v.add("The Old Man and The Sea"))

  s1.sync()

  # expected: { Hemingway: ["The Old Man and The Sea"] }
  print s1["map"]
  print s2["map"]


def main():
  test2()

if __name__ == "__main__":
  main()

