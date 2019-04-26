mpi-mlton
======


MPI (http://www.mpi-forum.org/) is a popular standard for
distributed-memory parallel programming. It offers both point-to-point
message passing and group communication operations (broadcast,
scatter/gather, etc).

Open MPI (http://www.open-mpi.org/) is an implementation of the MPI
standard that combines technologies and resources from several other
projects (FT-MPI, LA-MPI, LAM/MPI, and PACX-MPI) in order to build the
best MPI library available.

The MLton MPI bindings provide an ML interface to a subset of the MPI
1.2 procedures for communication.  It is based on the Ocaml MPI
library by Xavier Leroy
(http://pauillac.inria.fr/~xleroy/software.html#ocamlmpi). The mpi
library has been tested with Open MPI and MPICH.
