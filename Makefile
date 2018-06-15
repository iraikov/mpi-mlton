MPI_LIB ?= mpich
MPI_INCLUDE_DIR ?= /usr/include/mpich

mpitest: mpitest.mlb  mpilib.c
	mlton -const 'Exn.keepHistory true' -export-header "mpiexport.h"  -cc-opt "-I${MPI_INCLUDE_DIR}" -link-opt "-l${MPI_LIB}" $< mpilib.c
mpi: mpi.mlb mpilib.c
	mlton -export-header "mpiexport.h"  -cc-opt "-I${MPI_INCLUDE_DIR}" -link-opt "-l${MPI_LIB}" $< mpilib.c
