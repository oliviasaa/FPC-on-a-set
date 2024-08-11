# FPC-on-a-set

This repository is a Rust implementation of Fast Probabilistic Consensus on a Set (Nitchai et al), intended for simulation and statistical analysis purposes.

Its current state is still WIP. The following features will be added in the near future:

- Addition of new nodes and transactions after the protocol had already started.
- Partial visions of nodes
- New malicious strategies
- New initial opinion distributions
- Support to random graphs

The following features are already implemented

- FPCS algorithm for complete graphs of nodes, all with a complete vision of the transactions.
- Conflict graph: complete or star
- Initial opinion distributions: equally distributed or concentrated in some nodes
- Type of nodes: honest, faulty, or malicious (single malicious strategy)


# How to use

Running the crate will initialize the database and run the FPCS algorithm until its finalization, printing the finalization results. 
The simulation parameters should be passed to the `Database::generate_new function` (main.rs).
