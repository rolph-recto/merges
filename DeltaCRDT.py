#!/usr/bin/env python
# DeltaCRDT.py
# From Delta State Replicated Datatypes, Almeida et al 2018

from CRDT import *

class DotStore(object):
  def dots(self):
    raise Exception("dots method not implemented!")

class DotSet(DotStore):
  def __init__(self, s=None):
    self.set = set() if s is None else s

  def dots(self):
    return self.dotset

  def copy(self):
    return DotSet(self.set.copy())

  def __str__(self):
    return "DotSet: " + str(self.set)


# map from dots to values
class DotFun(DotStore):
  def __init__(self, m=None):
    self.map = {} if m is None else m

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
  def __init__(self, m=None):
    self.map = {} if m is None else m

  def dots(self):
    d = set()
    for v in self.map.values():
      d |= v.dots()

    return d

  def __setitem__(self, dot, val):
    self.map[dot] = val

  def __getitem__(self, dot):
    return self.map[dot]

  def copy(self):
    return DotMap(self.map.copy())
  
  def __str__(self):
    return "DotMap: " + str(self.map)

    
class CausalCRDT(CRDT):
  def __init__(self, name, ds):
    super(CausalCRDT, self).__init__(name)
    self.dot_store = ds
    self.causal_ctx = set()

  def copy(self):
    clone = CausalCRDT(self.dot_store.copy())
    clone.causal_ctx = self.causal_ctx.copy()
    return clone

  # is the CRDT's state the bottom of the lattice?
  def bottom(self):
    raise Exception("bottom() must be implemented")

  def join(self, ccrdt):
    if type(self.dot_store) is DotSet:
      m = (ccrdt.dot_store & self.dot_store) \
        | (self.dot_store - ccrdt.causal_ctx) \
        | (ccrdt.dot_store - self.causal_ctx)
      self.dot_store = DotStore(m)
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
      union_keys = self.dot_store.dots() | ccrdt.dot_store.dots()
      m = {}
      for key in union_keys:
        if key in self.dot_store.map and key not in ccrdt.dot_store.map:
          m[key] = self.dot_store.map[key]

        elif key not in self.dot_store.map and key in ccrdt.dot_store.map:
          m[key] = ccrdt.dot_store.map[key]

        else:
          clone = self.copy()
          clone.join(ccrdt)
          if not clone.bottom():
            m[key] = clone.dot_store.map[key]

      self.dot_store = DotMap(m)
      self.causal_ctx |= ccrdt.causal_ctx

  def __str__(self):
    return "CausalCRDT\n" + str(self.dot_store) + "\nCausal context: " + str(self.causal_ctx)


class MVRegister(CausalCRDT):
  def __init__(self, name):
    super(MVRegister, self).__init__(name, DotFun())

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

  # unlike op-based CRDTs, there is a single executor, which just joins
  # the delta from the operation to the current state of the CRDT.
  @CRDT.executor("MVRegister")
  def join(self, data):
    super(MVRegister, self).join(data["delta"])


def test1():
  s1 = CRDTStore()
  s1.register(MVRegister("reg"))
  s1["reg"].write(10)

  s2 = CRDTStore()
  s1.connect(s2)
  s2.register(MVRegister("reg"))
  s2["reg"].write(20)
  s2["reg"].write(30)

  s1.sync()

  print s1["reg"].read()

def main():
  test1()


if __name__ == "__main__":
  main()
