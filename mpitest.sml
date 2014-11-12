
val _ = MPI.Init (CommandLine.arguments())
val _ = print ("Wtime = " ^ (Real.toString (MPI.Wtime ())) ^ "\n")

val size = MPI.Comm.Size (MPI.Comm.World ())
val _ = print ("World size = " ^ (Int.toString (size)) ^ "\n")

val myrank = MPI.Comm.Rank (MPI.Comm.World ())
val _ = print ("My rank = " ^ (Int.toString (myrank)) ^ "\n")

fun mpiPrint str = print ("Rank " ^ (Int.toString myrank) ^ ": " ^ str)

fun charArrayString (a) = String.implode (CharArray.foldr (op ::) [] a)

val _ = 
    (if myrank = 0
     then 
         (let
             val data = "ab"
             val r = CharArray.fromList (String.explode data)
	     val _ = mpiPrint ("sending " ^ data ^ "\n")
	     val _ = MPI.Message.Send(MPI.MPI_CHAR_ARRAY (r), 1, 0, MPI.Comm.World())
             val (n,_,_) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World())
	     val _ = mpiPrint ("received " ^ (Int.toString n) ^ " bytes\n")
             val r1 = CharArray.array (n, Char.chr 0)
             val n = MPI.Message.Recv (MPI.MPI_CHAR_ARRAY (r1), 
                                       MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World())
         in 
	     mpiPrint ("received data = " ^ (charArrayString r1) ^ "\n")
         end)
     else
         (let
             val (n,_,_) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World())
	     val _ = mpiPrint ("received " ^ (Int.toString n) ^ " bytes\n")
             val r = CharArray.array (n, Char.chr 0)
             val _ = MPI.Message.Recv (MPI.MPI_CHAR_ARRAY (r), 
                                       MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World())
             val r1 = (charArrayString r) ^ "ab"
	     val _ = MPI.Message.Send(MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode r1)), 
                                      Int.mod (myrank + 1, size), 0, MPI.Comm.World())
             
         in 
            mpiPrint ("received " ^ (charArrayString r) ^ ", resending " ^ r1 ^ "\n")
         end)
    )

val _ = print ("Barrier = " ^ (Int.toString (MPI.Barrier (MPI.Comm.World ()))) ^ "\n")
val _ = MPI.Finalize ()
