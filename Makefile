all: mpi.mlb
	mlton -export-header "mpiexport.h"  -cc-opt "-I/usr/include/openmpi" -link-opt "-lmpi" $< mpilib.c
