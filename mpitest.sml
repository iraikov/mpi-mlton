
exception AssertionFailed

fun assert true = ()
  | assert false = raise AssertionFailed

val _ = MPI.Init (CommandLine.arguments())
val _ = print ("Wtime = " ^ (Real.toString (MPI.Wtime ())) ^ "\n")

val size = MPI.Comm.Size (MPI.Comm.World)
val _ = print ("World size = " ^ (Int.toString (size)) ^ "\n")

val myrank = MPI.Comm.Rank (MPI.Comm.World)
val _ = print ("My rank = " ^ (Int.toString (myrank)) ^ "\n")

fun mpiPrint str = print ("Rank " ^ (Int.toString myrank) ^ ": " ^ str)
fun mpiPrintLn str = print ("Rank " ^ (Int.toString myrank) ^ ": " ^ str ^ "\n")

exception InvalidCharArray
exception InvalidIntArray

fun showIntArray (v) =
    (String.concatWith ", " (Array.foldr (fn (x, ax) => (Int.toString x)::ax) [] v))
            


fun testSendRecv (pu,data,transf) =
    if (myrank = 0)
     then 
        (let
            fun sendloop (lst, i) =
                if (not (List.null lst)) andalso (i < size)
                then
		    (mpiPrintLn ("sending data to " ^ (Int.toString i));
		     MPI.Message.Send pu (hd lst, i, 0, MPI.Comm.World);
		     sendloop (tl lst, i+1))
                else ()

            fun recvloop (i) =
		if ((i - 1) > 0)
                then 
		    (let
                        val x = MPI.Message.Recv pu (i-1, 0, MPI.Comm.World)
			 (*(test-assert (any (lambda (y) (equal? x y)) (map transf data)))*)
                    in
			recvloop (i-1)
                    end)
                else ()

        in
            sendloop (data,1);
            recvloop (size)
        end)
    else 
	(let
            val x = MPI.Message.Recv pu (0, 0, MPI.Comm.World)
             (*(test-assert (member x data))*)
	    val y = transf x
        in
	    MPI.Message.Send pu (y, 0, 0, MPI.Comm.World);
            ()
        end)


fun testBcast data =
    if (myrank = 0)
    then (let
             val _ = mpiPrintLn ("broadcasting data")
             val result = MPI.Collective.Bcast (Pickle.string) (SOME data, 0, MPI.Comm.World)
         in
             ()
         end)
    else (let
             val _ = mpiPrintLn ("receiving broadcast data")
             val result = MPI.Collective.Bcast (Pickle.string) (NONE, 0, MPI.Comm.World)
         in
             mpiPrintLn ("received broadcast: " ^ result)
         end)
             
fun testScatter data =
    if (myrank = 0)
     then (let val _ = mpiPrintLn ("scatter send")
               val a = MPI.Collective.Scatter Pickle.string (SOME (List.tabulate (size, fn(i) => data ^ " " ^ (Int.toString i))), 0, MPI.Comm.World)
           in
               mpiPrintLn ("scatter received: " ^ a)
           end)
     else (let val _ = mpiPrintLn ("scatter receive")
               val a = MPI.Collective.Scatter Pickle.string (NONE, 0, MPI.Comm.World) 
           in
               mpiPrintLn ("scatter received: " ^ a)
           end)
              


fun testScatterv data =
    if (myrank = 0)
    then (let 
             val a = MPI.Collective.Scatterv Pickle.string (SOME data, 0, MPI.Comm.World)
         in
             mpiPrintLn ("scatterv received " ^ a)
         end)
    else (let 
             val a = MPI.Collective.Scatterv Pickle.string (NONE, 0, MPI.Comm.World) 
         in
             mpiPrintLn ("scatterv received " ^ a)
         end)



fun testGather data = 
    if (myrank = 0)
    then (let val _ = mpiPrintLn "gather receive"
              val a = MPI.Collective.Gather Pickle.string (data, 0, MPI.Comm.World)
           in
               mpiPrintLn ("gather received " ^ (String.concat a))
           end)
     else (let val _ = mpiPrintLn "gather send"
               val _ = MPI.Collective.Gather Pickle.string (data, 0, MPI.Comm.World) 
           in
               ()
           end)

fun testGatherv data =
    if (myrank = 0)
     then (let val _ = mpiPrintLn "gatherv receive"
               val a = MPI.Collective.Gatherv Pickle.string (data, 0, MPI.Comm.World)
           in
               mpiPrintLn ("gatherv received " ^ (String.concat a))
           end)
    else (let val _ = mpiPrintLn "gatherv send"
              val _ = MPI.Collective.Gatherv Pickle.string (data, 0, MPI.Comm.World) 
           in
               ()
          end)
             
fun testAllgather data = 
    let val _ = mpiPrintLn "allgather receive"
        val a = MPI.Collective.Allgather Pickle.string (data, MPI.Comm.World)
    in
        mpiPrintLn ("allgather received " ^ (String.concat a))
    end

fun testAllgatherv data =
    let val _ = mpiPrintLn "allgatherv receive"
        val a = MPI.Collective.Allgatherv Pickle.string (data, MPI.Comm.World)
    in
        mpiPrintLn ("allgatherv received " ^ (String.concat a))
    end


             
fun testAlltoall data =
    let val _ = mpiPrintLn ("alltoall send")
        val a = MPI.Collective.Alltoall Pickle.string (List.tabulate (size, fn(i) => data ^ " " ^ (Int.toString i)), MPI.Comm.World)
    in
        mpiPrintLn ("alltoall received: " ^ (String.concat a))
    end

fun testAlltoallv data =
    let val _ = mpiPrintLn ("alltoallv send")
        val a = MPI.Collective.Alltoallv Pickle.string (List.tabulate (size, fn(i) => data ^ " " ^ (Int.toString i)), MPI.Comm.World)
    in
        mpiPrintLn ("alltoallv received " ^ (String.concat a))
    end

        
val _ = 
    (if myrank = 0
     then 
         (let
             val data = "ab"
	     val _ = mpiPrintLn ("sending " ^ data)
	     val _ = MPI.Message.Send (Pickle.string) (data, 1, 0, MPI.Comm.World)
             val (n,_,_) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
	     val _ = mpiPrintLn ("received " ^ (Int.toString n) ^ " bytes")
             val s = MPI.Message.Recv (Pickle.string) (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
         in 
	     mpiPrintLn ("received data = " ^ s)
         end)
     else
         (let
             val (n,_,_) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
	     val _ = mpiPrintLn ("received " ^ (Int.toString n) ^ " bytes")
             val r = MPI.Message.Recv (Pickle.string) (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r1 = r ^ "ab"
	     val _ = MPI.Message.Send (Pickle.string) (r1, Int.mod (myrank + 1, size), 0, MPI.Comm.World)
             
         in 
            mpiPrintLn ("received " ^ r ^ ", resending " ^ r1)
         end)
    )

val _ = MPI.Barrier MPI.Comm.World
val _ = 
    (if myrank = 0
     then 
         (let
             val data1 = "aa"
             val data2 = "bb"

	     val _ = mpiPrintLn ("sending (tag 0) " ^ data1)
	     val _ = MPI.Message.Send (Pickle.string) (data1, 1, 0, MPI.Comm.World)

	     val _ = mpiPrintLn ("sending (tag 1) " ^ data2)
	     val _ = MPI.Message.Send (Pickle.string) (data2, 1, 1, MPI.Comm.World)

             val (n,src,tag) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r1 = MPI.Message.Recv (Pickle.string) (src, tag, MPI.Comm.World)
	     val _ = mpiPrintLn ("received " ^ r1 ^ ", " ^ (Int.toString n) ^ " bytes, " ^ 
                                 "tag " ^ (Int.toString tag) ^ " " ^
                                 "from " ^ (Int.toString src))

             val (n,src,tag) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r2 = MPI.Message.Recv (Pickle.string) (src, tag, MPI.Comm.World)
	     val _ = mpiPrintLn ("received " ^ r2 ^ ", " ^ (Int.toString n) ^ " bytes, " ^ 
                                 "tag " ^ (Int.toString tag) ^ " " ^
                                 "from " ^ (Int.toString src))
         in
             ()
		        (*(if (zero? tag) 
			(test-assert (check-string myrank (blob->string n) #\a size))
			(test-assert (check-string myrank (blob->string n) #\b size)))*)
         end)
     else 
         (let 
             val (n1,src1,tag1) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r1 = MPI.Message.Recv (Pickle.string) (src1, tag1, MPI.Comm.World)
             val r1' = r1 ^ (if tag1 = 0 then "a" else "b")
	     val _ = mpiPrintLn ("received " ^ r1 ^ ", " ^ (Int.toString n1) ^ " bytes, " ^ 
                                 "tag " ^ (Int.toString tag1) ^ " " ^
                                 "from " ^ (Int.toString src1) ^ ", resending " ^ r1')
	     (*(if (zero? tag1)
		 (test-assert (check-string myrank n1 #\a myrank))
		 (test-assert (check-string myrank n1 #\b myrank)))*)

             val (n2,src2,tag2) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r2 = MPI.Message.Recv (Pickle.string) (src2, tag2, MPI.Comm.World)
             val r2' = r2 ^ (if tag2 = 0 then "a" else "b")
	     val _ = mpiPrintLn ("received " ^ r2 ^ ", " ^ (Int.toString n2) ^ " bytes, " ^ 
                                 "tag " ^ (Int.toString tag2) ^ " " ^
                                 "from " ^ (Int.toString src2) ^ ", resending " ^ r2')

			        (*(if (zero? tag2)
			       (test-assert (check-string myrank n2 #\a myrank))
			       (test-assert (check-string myrank n2 #\b myrank)))*)
         in
	     (MPI.Message.Send (Pickle.string) (r1', Int.mod (myrank + 1, size), tag1, MPI.Comm.World);
	      MPI.Message.Send (Pickle.string) (r2', Int.mod (myrank + 1, size), tag2, MPI.Comm.World);
              ())
         end))

val _ = MPI.Barrier MPI.Comm.World



val intdata  = List.tabulate (size, fn (i) => (10 * i))
val realdata = List.tabulate (size, fn (i) => (Real.* (0.1, Real.fromInt i)))
val vsize    = 3
val vsdata   = List.tabulate (size, fn (i) => (String.implode 
                                                   (List.tabulate
                                                        (vsize,
							 (fn (j) => (Char.chr (i + 97)))))))
val vvsdata   = List.tabulate (size, fn (i) => (String.implode 
                                                    (List.tabulate
                                                         (vsize+i,
							  (fn (j) => (Char.chr (i + 97)))))))

val _ = testSendRecv (Pickle.int, intdata, fn(x) => x+1)

val _ = testSendRecv (Pickle.real, realdata, fn(x) => Real.+(x,1.0))

val _ = testBcast "Hello!"

val _ = testScatter "Hello"

val _ = testScatterv vvsdata

val _ = testGather (List.nth (vsdata, myrank))

val _ = testGatherv (List.nth (vvsdata, myrank))

val _ = testAllgather (List.nth (vsdata, myrank))

val _ = testAllgatherv (List.nth (vvsdata, myrank))

val _ = testAlltoall (List.nth (vsdata, myrank))

val _ = testAlltoallv (List.nth (vvsdata, myrank))
                    
val _ = MPI.Finalize ()
