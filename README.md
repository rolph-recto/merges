# Merges

Code for merges research. Current contents:

### CRDT.py

Toy CRDT implementation. Models synchronization across several stores/replicas.

*OpCRDT.py* contains implementations of op-based CRDTs

*DeltaCRDT.py* contains implementations of delta state-based CRDTs, from
"Delta State Replicated Data Types" by Almeida et al, 2016


### CRDT.hs

Model of CRDTs with algebraic effects. Despite the name, there aren't actually
CRDTs here (yet) -- this is more concerned with figuring out the semantics
of handlers when CRDTs compose.
