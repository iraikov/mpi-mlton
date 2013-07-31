mpi
======


[[http://www-unix.mcs.anl.gov/mpi/|MPI]] is a popular library for
distributed-memory parallel programming. It offers both point-to-point
message passing and group communication operations (broadcast,
scatter/gather, etc).

[[http://www.open-mpi.org/|Open MPI]] is an implementation of the MPI
standard that combines technologies and resources from several other
projects (FT-MPI, LA-MPI, LAM/MPI, and PACX-MPI) in order to build the
best MPI library available.

The MLton MPI bindings provide an ML interface to a subset of
the MPI 1.2 procedures for communication.  It is based on the
[[http://pauillac.inria.fr/~xleroy/software.html#ocamlmpi|Ocaml MPI]]
library by Xavier Leroy. The mpi library has been tested with Open
MPI version 1.6.
