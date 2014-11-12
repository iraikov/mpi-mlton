
val _ = MPI.Init (CommandLine.arguments())
val _ = print ("Wtime = " ^ (Real.toString (MPI.Wtime ())) ^ "\n")
val _ = print ("World size = " ^ (Int.toString (MPI.Comm.Size (MPI.Comm.World ()))) ^ "\n")
val _ = print ("My rank = " ^ (Int.toString (MPI.Comm.Rank (MPI.Comm.World ()))) ^ "\n")
val _ = print ("Barrier = " ^ (Int.toString (MPI.Barrier (MPI.Comm.World ()))) ^ "\n")
val _ = MPI.Finalize ()
