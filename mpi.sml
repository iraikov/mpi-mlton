
structure MPI =
struct

exception MPIError of int * string
                      
structure P = MLton.Pointer
              
type comm  = P.t
type group = P.t

structure WordArray = Word8Array
structure WordArraySlice = Word8ArraySlice
structure WordVector = Word8Vector
structure WordVectorSlice = Word8VectorSlice


exception AssertionFailed

fun assert true = ()
  | assert false = raise AssertionFailed

                 
val e = _export "mlton_MPI_exception": (int * int * string -> unit) -> unit;

val _ = e (fn (i, len, msg) => raise MPIError (i, msg))
          
val cInit = _import "mlton_MPI_Init" : int * string array * int array -> unit;

fun Init (args) =
  let
      val argc   = List.length args
      val argv   = Array.fromList args
      val argvsz = Array.fromList (map String.size args)
  in
      cInit (argc, argv, argvsz)
  end

val Success    = 0
val Finalize   = _import "MPI_Finalize" : unit -> unit;

val Wtime      = _import "MPI_Wtime": unit -> real ;

val Barrier    = _import "MPI_Barrier" : comm -> int ;
 
 structure Comm =
 struct

   val World  = 
       let 
           val f = _import "mlton_MPI_comm_world" : unit -> comm ;
       in
           f()
       end

   val cSize  = _import "MPI_Comm_size" :  comm * int ref  -> int ;

   fun Size (comm) =
       let 
           val i = ref ~1
       in
           cSize (comm, i); !i
       end

   val cRank  = _import "MPI_Comm_rank" :  comm * int ref  -> int;

   fun Rank (comm) =
       let 
           val i = ref ~1
       in
           cRank (comm, i); !i
       end


   val cCompare  = _import "MPI_Comm_compare" :  comm * comm * int ref  -> int;

   fun Equal (comm1,comm2) =
       let 
           val i = ref ~1
       in
           cCompare (comm1, comm2, i); ((!i) = 0)
       end


   val cCreate  = _import "MPI_Comm_create" :  comm * group * comm ref  -> int;

   fun Create (comm,group) =
       let 
           val p = ref P.null
       in
           cCreate (comm, group, p); !p
       end


   val cSplit  = _import "MPI_Comm_split" :  comm * int * int * comm ref -> int;

   fun Split (comm,color,key) =
       let
           val p = ref P.null
       in
           cSplit (comm,color,key,p); !p
       end


   val cCartCreate  = _import "MPI_Cart_create" :  comm * int * Int32Array.array * Int32Array.array * bool * comm ref -> int;

   fun CartCreate (comm,dims,periods,reorder) =
       let
           val p = ref P.null
           val ndims = Int32Array.length dims
       in
           cCartCreate (comm, ndims, dims, periods, reorder, p); !p
       end


   val cDimsCreate  = _import "MPI_Dims_create" :  int * int * Int32Array.array -> int;

   fun DimsCreate (nnodes, ndims) =
       let
           val v = Int32Array.array (ndims, ~1)
       in
           cDimsCreate (nnodes,ndims,v); v
       end

   val cCartRank  = _import "MPI_Cart_rank" : comm * Int32Array.array * int ref -> int ;

   fun CartRank (comm, coords) =
       let
           val i = ref ~1
       in
           cCartRank (comm, coords, i); !i
       end


   val cCartdimGet = _import "MPI_Cartdim_get" : comm * int ref -> int ;

   fun CartdimGet (comm) =
       let
           val i = ref ~1
       in
           cCartdimGet (comm, i); !i
       end

   val cCartCoords = _import "MPI_Cart_coords" : comm * int * int * Int32Array.array -> int ;

   fun CartCoords (comm,rank) =
       let
           val ndims = CartdimGet comm
           val coords = Int32Array.array (ndims,~1)
       in
           cCartCoords (comm, rank, ndims, coords); coords
       end

 end


 structure Group =
 struct

   val cSize   = _import "MPI_Group_size" :  group * int ref  -> int;

   fun Size (group) =
       let
           val sz = ref ~1
       in
           cSize (group,sz); !sz
       end


   val cRank   = _import "MPI_Group_rank" :  group * int ref  -> int;

   fun Rank (group) =
       let
           val rank = ref ~1
       in
           cRank (group,rank); !rank
       end


   val cTranslateRanks = _import "MPI_Group_translate_ranks" :  group * int * Int32Array.array * group * Int32Array.array -> int ;

   fun TranslateRanks (group1, ranks1, group2) =
       let
           val nranks = Int32Array.length ranks1
           val ranks2 = Int32Array.array (nranks, ~1)
       in
           cTranslateRanks (group1, nranks, ranks1, group2, ranks2)
       end


   val cCommGroup = _import "MPI_Comm_group" :  group * group ref -> int ;

   fun CommGroup (comm) =
       let
           val p = ref P.null
       in
           cCommGroup (comm,p); !p
       end


   val cUnion = _import "MPI_Group_union" :  group * group * group ref  -> int ;

   fun Union (group1,group2) =
       let
           val p = ref P.null
       in
           cUnion (group1,group2,p); !p
       end


   val cDiff = _import "MPI_Group_difference" :  group * group * group ref  -> int ;

   fun Diff (group1,group2) =
       let
           val p = ref P.null
       in
           cDiff (group1,group2,p); !p
       end


   val cIntersection = _import "MPI_Group_intersection" :  group * group * group ref  -> int ;

   fun Intersection (group1,group2) =
       let
           val p = ref P.null
       in
           cIntersection (group1,group2,p); !p
       end


   val cIncl = _import "MPI_Group_incl" :  group * int * Int32Array.array * group ref  -> int ;

   fun Incl (group,ranks) =
       let
           val p = ref P.null
           val nranks = Int32Array.length ranks
       in
           cIncl (group,nranks,ranks,p); !p
       end


   val cExcl = _import "MPI_Group_excl" :  group * int * Int32Array.array * group ref  -> int ;

   fun Excl (group,ranks) =
       let
           val p = ref P.null
           val nranks = Int32Array.length ranks
       in
           cExcl (group,nranks,ranks,p); !p
       end

 end

 structure Message =
 struct


    val AnyTag = 
        let 
            val f = _import "mlton_MPI_get_any_tag" : unit -> int;
        in
            f()
        end

    val AnySource = 
        let
            val f = _import "mlton_MPI_get_any_source" : unit -> int;
        in
            f()
        end

    val cProbe = _import "mlton_MPI_probe" : int ref * int ref * int ref * int * int * comm -> unit;

    fun Probe (source, tag, comm) =
        let
            val status_count = ref ~1
            val status_source = ref ~1
            val status_tag = ref ~1
        in
            cProbe (status_count, status_source, status_tag,
                    source, tag, comm);
            ((!status_count), (!status_source), (!status_tag))
        end


    val cSendW8 = _import "mlton_MPI_Send_Word8" : WordVector.vector * int * int * int * comm -> int;

    val cRecvW8 = _import "mlton_MPI_Recv_Word8" : WordArray.array * int * int * int * comm -> int;


    fun Send (pu: 'a Pickle.pu) (data: 'a, dest, tag, comm) =
      let
          val buf = Pickle.pickle pu data
          val n = WordVector.length buf
      in
          cSendW8 (buf, n, dest, tag, comm)
      end

    fun Recv (pu: 'a Pickle.pu) (source, tag, comm) =
      let
          val (n, _, _) = Probe (source, tag, comm)
          val r = WordArray.array (n, 0w0)
          val status = cRecvW8 (r, n, source, tag, comm)
      in
          if (not (status = 0))
          then raise MPIError (status, "Recv error")
          else Pickle.unpickle pu (WordArray.vector r)
      end
 end
     
 structure Collective =
 struct


    val cBcastW8 = _import "mlton_MPI_Bcast_Word8" : WordVector.vector * int * WordArray.array * int * comm -> int;


    val cScatterW8 = _import "mlton_MPI_Scatter_Word8"   : WordVector.vector * int * WordArray.array * int * int * comm -> int;

    val cScattervW8 = _import "mlton_MPI_Scatterv_Word8"   : WordVector.vector * IntArray.array * IntArray.array * WordArray.array * int * int * comm -> int;

    val cGatherW8 = _import "mlton_MPI_Gather_Word8"   : WordVector.vector * int * WordArray.array * int * int * comm -> int;

    val cGathervW8 = _import "mlton_MPI_Gatherv_Word8"   : WordVector.vector * int * WordArray.array * IntArray.array * IntArray.array * int * comm -> int;

    exception InvalidScatter
    exception InvalidScatterDataSize of int * int * int

    exception InvalidGather
    exception InvalidGatherDataSize of int * int * int

    fun Bcast  (pu: 'a Pickle.pu) (data, root, comm) =
      let
          val myrank = Comm.Rank comm
      in
          if myrank = root
          then
              (let
                  val input   = valOf data
                  val buf     = Pickle.pickle pu input
                  val szbuf   = Pickle.pickle Pickle.int (WordVector.length buf)
                  val recvbuf = WordArray.array(WordVector.length szbuf, 0w0)
                  val status  = cBcastW8 (szbuf, WordVector.length szbuf, recvbuf, root, comm)
                  val _       = assert (status = Success)
                  val sz      = Pickle.unpickle Pickle.int (WordArray.vector recvbuf)
                  val recvbuf = WordArray.array(sz, 0w0)
                  val status  = cBcastW8 (buf, sz, recvbuf, root, comm)
                  val _       = assert (status = Success)
              in
                  Pickle.unpickle pu (WordArray.vector recvbuf)
              end)
          else (let
                  val szbuf   = Pickle.pickle Pickle.int 0
                  val recvbuf = WordArray.array(WordVector.length szbuf, 0w0)
                  val status  = cBcastW8 (szbuf, WordVector.length szbuf, recvbuf, root, comm)
                  val _       = assert (status = Success)
                  val sz      = Pickle.unpickle Pickle.int (WordArray.vector recvbuf)
                  val recvbuf = WordArray.array(sz, 0w0)
                  val status  = cBcastW8 (szbuf, sz, recvbuf, root, comm)
                  val _       = assert (status = Success)
               in
                   Pickle.unpickle pu (WordArray.vector recvbuf)
               end)
      end
                                            
                                         

              
    fun Scatter (pu: 'a Pickle.pu) (data: 'a list option, root, comm): 'a =
      let
          val myrank = Comm.Rank comm
      in
          if myrank = root
          then (let 
                   val nprocs = Comm.Size comm

                   val lst = valOf data
                   val _ = assert((List.length lst) = nprocs)
                                 
                   val pkls = List.map (Pickle.pickle pu) lst

                   val szbuf  = Pickle.pickle Pickle.int (WordVector.length (hd pkls))
                   val recvbuf = WordArray.array(WordVector.length szbuf, 0w0)

                   val status  = cBcastW8 (szbuf, WordVector.length szbuf, recvbuf, root, comm)
                   val _       = assert (status = Success)
                                                      
	           (* Allocates receive buffer *)
                   val sz  = Pickle.unpickle Pickle.int (WordArray.vector recvbuf)
                   val myrecvbuf = WordArray.array (sz, 0w0)
                                                   
                   (* Allocates and fills send buffer *)
                   val sendbuf = WordVectorSlice.concat (List.map (fn(v) => (WordVectorSlice.slice (v,0,SOME sz))) pkls)
                                                        
               in
	           (* Performs the scatter & returns received value *)
                   cScatterW8 (sendbuf, sz, myrecvbuf, sz, root, comm);
                   Pickle.unpickle pu (WordArray.vector myrecvbuf)
               end)
          else 
              (let
                  val szbuf   = Pickle.pickle Pickle.int 0
                  val recvbuf = WordArray.array(WordVector.length szbuf, 0w0)
                  val status  = cBcastW8 (szbuf, WordVector.length szbuf, recvbuf, root, comm)
                  val _       = assert (status = Success)
                  val sz      = Pickle.unpickle Pickle.int (WordArray.vector recvbuf)
                  val recvbuf = WordArray.array(sz, 0w0)
                  val status  = cScatterW8 (szbuf, sz, recvbuf, sz, root, comm)
                  val _       = assert (status = Success)
               in
                   Pickle.unpickle pu (WordArray.vector recvbuf)
              end)
      end
          
    fun Scatterv (pu: 'a Pickle.pu) (data: 'a list option, root, comm): 'a =
      let
          val myrank = Comm.Rank comm
      in
          if myrank = root
          then
              (let 
                  val nprocs = Comm.Size comm

                  val lst = valOf data
                                  
                  val _ = assert((List.length lst) = nprocs)
                                
                  val pkls = List.map (Pickle.pickle pu) lst
                  val sendlengths = List.map WordVector.length pkls

                  val displs = List.rev (#2(List.foldl (fn (len,(i,lst)) => (i+len,i :: lst)) (0,[]) sendlengths))

                  (* Scatters the lengths of the buffers to all the processes *)
                  val mylen = Scatter Pickle.int (SOME sendlengths, root, comm)
                                             
	          (* Allocates receive buffer *)
                  val myrecvbuf = WordArray.array (mylen, 0w0)
                                                  
                  (* Allocates and fills send buffer *)
                  val sendbuf = WordVectorSlice.concat (List.map (fn(v) => (WordVectorSlice.slice (v,0,NONE))) pkls)

                  
	          (* Performs the scatter & returns received value *)
                  val status  = cScattervW8 (sendbuf, IntArray.fromList sendlengths, IntArray.fromList displs, 
                                             myrecvbuf, mylen, root, comm)
                  val _       = assert (status = Success)
                  
              in
                  Pickle.unpickle pu (WordArray.vector myrecvbuf)
              end)
          else
              (let
                  val sendbuf = Pickle.pickle Pickle.int 0

                  (* Receives the length of the buffer for this process *)
                  val mylen = Scatter Pickle.int (NONE, root, comm)

	          (* Allocates receive buffer *)
                  val myrecvbuf = WordArray.array (mylen, 0w0)
                                                  
	          (* Performs the scatter & returns received value *)
                  val status = cScattervW8 (sendbuf, IntArray.fromList [], IntArray.fromList [], 
                                            myrecvbuf, mylen, root, comm)
                  val _      = assert (status = Success)
              in
                  Pickle.unpickle pu (WordArray.vector myrecvbuf)
              end)
      end

          
    fun Gather (pu: 'a Pickle.pu) (data: 'a, root, comm) =
      let 
          val nprocs = Comm.Size comm
          val myrank = Comm.Rank comm
          val mydata = Pickle.pickle pu data
          val mylen  = WordVector.length mydata
      in
          if myrank = root
          then
              (let
                  (* Allocate receive buffer *)
                  val recvbuf = WordArray.array (mylen * nprocs, 0w0)
                  (* Gather the data *)
                  val _ = cGatherW8 (mydata, mylen, recvbuf, mylen, root, comm)
                  (* Build a list of results and return *)
                  val uf = Pickle.unpickle pu
                  val results = List.tabulate
                                    (nprocs,
                                     fn(i) =>
                                        let
                                            val offset = i*mylen
                                            val s = WordArraySlice.slice (recvbuf, offset, SOME(offset+mylen))
                                        in
                                            uf (WordArraySlice.vector s)
                                        end)
              in
                  results
              end)
          else
              (let
                  val recvbuf = WordArray.array (0, 0w0)                  
                  val _ = cGatherW8 (mydata, mylen, recvbuf, mylen, root, comm)
              in
                  []
              end)
              
      end

    fun Gatherv (pu: 'a Pickle.pu) (data: 'a, root, comm) =
      let 
          val nprocs = Comm.Size comm
          val myrank = Comm.Rank comm
          val mydata = Pickle.pickle pu data
          val mylen  = WordVector.length mydata
          val recvlengths = Gather Pickle.int (mylen, root, comm)
      in
          if myrank = root
          then
              (let
                  (* Gather the data *)
                  val recvbuf = WordArray.array (List.foldl (op +) 0 recvlengths, 0w0)
                  val (_,displs)  = List.foldl (fn (len,(i,lst)) => (i+len,i :: lst)) (0,[]) recvlengths
                  val _ = cGathervW8 (mydata, mylen, recvbuf, IntArray.fromList recvlengths,
                                     IntArray.fromList displs, root, comm)
                  (* Build a list of results and return *)
                  val uf = Pickle.unpickle pu
                  val (results,_) = List.foldr
                                        (fn(count,(ax,offset)) =>
                                                 let
                                                     val s = WordArraySlice.slice (recvbuf, offset, SOME(offset+count))
                                                 in
                                                     ((uf (WordArraySlice.vector s))::ax, offset+count)
                                                 end)
                                        ([],0) recvlengths  
              in
                  results
              end)
          else
              (* If not root, send our length *)
              (let
                  val recvbuf = WordArray.array (0, 0w0)                  
                  val _ = cGathervW8 (mydata, mylen, recvbuf, IntArray.fromList [],
                                      IntArray.fromList [], root, comm)
              in
                  []
              end)
              
      end

 end

end
