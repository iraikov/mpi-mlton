
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

fun charArrayString (a) = 
    case a of
        MPI.MPI_CHAR_ARRAY r => 
        String.implode (CharArray.foldr (op ::) [] r)
      | _ => raise InvalidCharArray


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
		     MPI.Message.Send (hd lst, i, 0, MPI.Comm.World);
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
	    MPI.Message.Send (y, 0, 0, MPI.Comm.World);
            ()
        end);
     MPI.Barrier (MPI.Comm.World))


fun testBcast (data, len, make, toString) =
    (if (myrank = 0)
     then (let
              val _ = mpiPrintLn ("broadcasting data")
              val result = MPI.Collective.Bcast (data, 0, MPI.Comm.World)
          in
              assert (result = 0)
          end)
     else (let
              val _ = mpiPrintLn ("receiving broadcast data")
              val a = make len
              val result = MPI.Collective.Bcast (a, 0, MPI.Comm.World)
          in
              assert (result = 0);
              mpiPrintLn ("received " ^ (toString a))
          end);
     MPI.Barrier (MPI.Comm.World))


val _ = 
    (if myrank = 0
     then 
         (let
             val data = "ab"
             val r = CharArray.fromList (String.explode data)
	     val _ = mpiPrintLn ("sending " ^ data)
	     val _ = MPI.Message.Send(MPI.MPI_CHAR_ARRAY (r), 1, 0, MPI.Comm.World)
             val (n,_,_) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
	     val _ = mpiPrintLn ("received " ^ (Int.toString n) ^ " bytes")
             val s = MPI.Message.Recv (MPI.MPI_CHAR_ARRAY_t, 
                                       MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
         in 
	     mpiPrintLn ("received data = " ^ (charArrayString s))
         end)
     else
         (let
             val (n,_,_) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
	     val _ = mpiPrintLn ("received " ^ (Int.toString n) ^ " bytes")
             val r = MPI.Message.Recv (MPI.MPI_CHAR_ARRAY_t, 
                                       MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r1 = (charArrayString r) ^ "ab"
	     val _ = MPI.Message.Send(MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode r1)), 
                                      Int.mod (myrank + 1, size), 0, MPI.Comm.World)
             
         in 
            mpiPrintLn ("received " ^ (charArrayString r) ^ ", resending " ^ r1)
         end)
    )

val _ = MPI.Barrier MPI.Comm.World

val _ = 
    (if myrank = 0
     then 
         (let
             val data1 = "aa"
             val s1 = CharArray.fromList (String.explode data1)
             val data2 = "bb"
             val s2 = CharArray.fromList (String.explode data2)

	     val _ = mpiPrintLn ("sending (tag 0) " ^ data1)
	     val _ = MPI.Message.Send(MPI.MPI_CHAR_ARRAY (s1), 1, 0, MPI.Comm.World)

	     val _ = mpiPrintLn ("sending (tag 1) " ^ data2)
	     val _ = MPI.Message.Send(MPI.MPI_CHAR_ARRAY (s2), 1, 1, MPI.Comm.World)

             val (n,src,tag) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r1 = MPI.Message.Recv (MPI.MPI_CHAR_ARRAY_t, src, tag, MPI.Comm.World)
	     val _ = mpiPrintLn ("received " ^ (charArrayString r1) ^ ", " ^ (Int.toString n) ^ " bytes, " ^ 
                                 "tag " ^ (Int.toString tag) ^ " " ^
                                 "from " ^ (Int.toString src))

             val (n,src,tag) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r2 = MPI.Message.Recv (MPI.MPI_CHAR_ARRAY_t, src, tag, MPI.Comm.World)
	     val _ = mpiPrintLn ("received " ^ (charArrayString r2) ^ ", " ^ (Int.toString n) ^ " bytes, " ^ 
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
             val r1 = MPI.Message.Recv (MPI.MPI_CHAR_ARRAY_t, src1, tag1, MPI.Comm.World)
             val r1' = (charArrayString r1) ^ (if tag1 = 0 then "a" else "b")
	     val _ = mpiPrintLn ("received " ^ (charArrayString r1) ^ ", " ^ (Int.toString n1) ^ " bytes, " ^ 
                                 "tag " ^ (Int.toString tag1) ^ " " ^
                                 "from " ^ (Int.toString src1) ^ ", resending " ^ r1')
	     (*(if (zero? tag1)
		 (test-assert (check-string myrank n1 #\a myrank))
		 (test-assert (check-string myrank n1 #\b myrank)))*)

             val (n2,src2,tag2) = MPI.Message.Probe (MPI.Message.AnySource, MPI.Message.AnyTag, MPI.Comm.World)
             val r2 = MPI.Message.Recv (MPI.MPI_CHAR_ARRAY_t, src2, tag2, MPI.Comm.World)
             val r2' = (charArrayString r2) ^ (if tag2 = 0 then "a" else "b")
	     val _ = mpiPrintLn ("received " ^ (charArrayString r2) ^ ", " ^ (Int.toString n2) ^ " bytes, " ^ 
                                 "tag " ^ (Int.toString tag2) ^ " " ^
                                 "from " ^ (Int.toString src2) ^ ", resending " ^ r2')

			        (*(if (zero? tag2)
			       (test-assert (check-string myrank n2 #\a myrank))
			       (test-assert (check-string myrank n2 #\b myrank)))*)
         in
	     MPI.Message.Send(MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode r1')), 
                              Int.mod (myrank + 1, size), tag1, MPI.Comm.World);
	     MPI.Message.Send(MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode r2')), 
                              Int.mod (myrank + 1, size), tag2, MPI.Comm.World);
             ()
         end))

val _ = MPI.Barrier MPI.Comm.World



val intdata = List.tabulate (size, fn (i) => MPI.MPI_INT (10 * i))
val realdata = List.tabulate (size, fn (i) => MPI.MPI_REAL (Real.* (0.1, Real.fromInt i)))

val _ = testSendRecv (fn(src,tag,comm) => MPI.Message.Recv(MPI.MPI_INT_t,src,tag,comm),
                      fn (MPI.MPI_INT x) => MPI.MPI_INT (1 + x), intdata)
val _ = testSendRecv (fn(src,tag,comm) => MPI.Message.Recv(MPI.MPI_REAL_t,src,tag,comm),
                      fn (MPI.MPI_REAL x) => MPI.MPI_REAL (Real.* (2.0, x)), realdata)

val _ = testBcast (MPI.MPI_CHAR_ARRAY (CharArray.fromList (String.explode ("Hello!"))),
                   6, fn(len) => MPI.MPI_CHAR_ARRAY (CharArray.array (len, Char.chr 0)),
                   charArrayString)

val _ = MPI.Finalize ()
