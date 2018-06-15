
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
            


fun testSendRecv (recvdata, transf, data) =
    (if (myrank = 0)
     then 
        (let
            val _ = mpiPrintLn ("testSendRecv: data length = " ^ (Int.toString (List.length data)) ^ 
                                " " ^ " size = " ^ (Int.toString size))
            fun sendloop (lst, i) =
                if (not (List.null lst)) andalso (i < size)
                then
		    (mpiPrintLn ("sending data to " ^ (Int.toString i));
		     MPI.Message.Send (Pickle.string) (hd lst, i, 0, MPI.Comm.World);
		     sendloop (tl lst, i+1))
                else ()

            fun recvloop (i) =
		if ((i - 1) > 0)
                then 
		    (let
                        val x = recvdata (i-1, 0, MPI.Comm.World)
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
            val x = recvdata (0, 0, MPI.Comm.World)
             (*(test-assert (member x data))*)
	    val y = transf x
        in
	    MPI.Message.Send (Pickle.string) (y, 0, 0, MPI.Comm.World);
            ()
        end);
     MPI.Barrier (MPI.Comm.World))


fun testBcast (data, len, make) =
    (if (myrank = 0)
     then (let
              val _ = mpiPrintLn ("broadcasting data")
              val result = MPI.Collective.Bcast (Pickle.string) (data, 0, MPI.Comm.World)
          in
              assert (result = 0)
          end)
     else (let
              val _ = mpiPrintLn ("receiving broadcast data")
              val a = make len
              val result = MPI.Collective.Bcast (Pickle.string) (a, 0, MPI.Comm.World)
          in
              assert (result = 0);
              mpiPrintLn ("received broadcast: " ^ a)
          end);
     MPI.Barrier (MPI.Comm.World))
(*
fun testScatter (data, len, make, toString) =
    (if (myrank = 0)
     then (let val _ = mpiPrintLn ("scatter send: data = " ^ (toString data) ^ " len = " ^ (Int.toString len))
               val a = make len
               val a = MPI.Collective.sendScatter (data, len, a, 0, MPI.Comm.World)
           in
               mpiPrintLn ("scatter received " ^ (toString a))
           end)
     else (let val _ = mpiPrintLn ("scatter receive: data = " ^ (toString data) ^ " len = " ^ (Int.toString len))
               val a = make len
               val a = MPI.Collective.recvScatter (a, 0, MPI.Comm.World) 
           in
               mpiPrintLn ("scatter received " ^ (toString a))
           end);
     MPI.Barrier (MPI.Comm.World))

fun testGather (data, len, make, toString) =
    (if (myrank = 0)
     then (let val _ = mpiPrintLn ("gather receive: data = " ^ (toString data) ^ " len = " ^ (Int.toString len))
               val a = make (len * size)
               val a = MPI.Collective.recvGather (a, len, data, 0, MPI.Comm.World)
           in
               mpiPrintLn ("gather received " ^ (toString a))
           end)
     else (let val _ = mpiPrintLn ("gather send: data = " ^ (toString data) ^ " len = " ^ (Int.toString len))
               val a = MPI.Collective.sendGather (data, 0, MPI.Comm.World) 
           in
               mpiPrintLn ("gather sent status " ^ (Int.toString a))
           end);
     MPI.Barrier (MPI.Comm.World))

fun testScatterv (data, toString) =
    (if (myrank = 0)
     then (let 
               val a = MPI.Collective.sendScatterv (data, 0, MPI.Comm.World)
           in
               mpiPrintLn ("scatterv received " ^ (toString a))
           end)
     else (let 
               val a = MPI.Collective.recvScatterv (MPI.MPI_CHAR_ARRAY_t, 0, MPI.Comm.World) 
           in
               mpiPrintLn ("scatterv received " ^ (toString a))
           end);
     MPI.Barrier (MPI.Comm.World))

fun testGatherv (data, toString) =
    (if (myrank = 0)
     then (let val _ = mpiPrintLn ("gatherv receive: data = " ^ (toString data))
               val (a,rlens) = MPI.Collective.recvGatherv (data, 0, MPI.Comm.World)
           in
               mpiPrintLn ("gatherv received " ^ (toString a));
               mpiPrintLn ("gatherv lengths: " ^ (showIntArray rlens))
           end)
     else (let val _ = mpiPrintLn ("gatherv send: data = " ^ (toString data))
               val a = MPI.Collective.sendGatherv (data, 0, MPI.Comm.World) 
           in
               mpiPrintLn ("gatherv sent status " ^ (Int.toString a))
           end);
     MPI.Barrier (MPI.Comm.World))
*)

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
(*
val _ = MPI.Barrier MPI.Comm.World



val intdata  = List.tabulate (size, fn (i) => MPI.MPI_INT (10 * i))
val realdata = List.tabulate (size, fn (i) => MPI.MPI_REAL (Real.* (0.1, Real.fromInt i)))
val vsize    = 3
val vsdata   = List.tabulate (size, fn (i) => (String.implode 
                                                   (List.tabulate
                                                        (vsize,
							 (fn (j) => (Char.chr (i + 97)))))))
val vvsdata   = List.tabulate (size, fn (i) => (String.implode 
                                                    (List.tabulate
                                                         (vsize+i,
							  (fn (j) => (Char.chr (i + 97)))))))

val _ = testSendRecv (fn(src,tag,comm) => MPI.Message.Recv(MPI.MPI_INT_t,src,tag,comm),
                      fn (MPI.MPI_INT x) => MPI.MPI_INT (1 + x), intdata)
val _ = testSendRecv (fn(src,tag,comm) => MPI.Message.Recv(MPI.MPI_REAL_t,src,tag,comm),
                      fn (MPI.MPI_REAL x) => MPI.MPI_REAL (Real.* (2.0, x)), realdata)

val _ = testBcast (MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode ("Hello!"))),
                   6, fn(len) => MPI.MPI_CHAR_ARRAY (CharArray.array (len, Char.chr 0)),
                   charArrayString)

val _ = testScatter (MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode (String.concat vsdata))),
                     String.size (String.concat vsdata) div size,
                     fn(len) => MPI.MPI_CHAR_ARRAY (CharArray.array (len, Char.chr 0)),
                     charArrayString)

val _ = testGather (MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode (List.nth(vsdata,myrank)))),
                    vsize,
                    fn(len) => MPI.MPI_CHAR_ARRAY (CharArray.array (len, Char.chr 0)),
                    charArrayString)

val _ = testScatterv (map (fn(s) => MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode (s)))) vvsdata,
                     charArrayString)

val _ = testGatherv (MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode (List.nth(vvsdata,myrank)))),
                     charArrayString)
*)
val _ = MPI.Finalize ()
