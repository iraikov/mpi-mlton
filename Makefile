mpitest: mpitest.mlb  mpilib.c
	mlton -const 'Exn.keepHistory true' -export-header "mpiexport.h"  -cc-opt "-I/usr/include/mpich" -link-opt "-lmpi" $< mpilib.c
mpi: mpi.mlb mpilib.c
	mlton -export-header "mpiexport.h"  -cc-opt "-I/usr/include/mpich" -link-opt "-lmpi" $< mpilib.c
