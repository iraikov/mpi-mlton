
structure MPI =
struct

exception MPIError of int * string
                      
structure P = MLton.Pointer
              
type comm  = P.t
type group = P.t

structure CharVector = MonoVector (type elem = CharVector.elem)
structure CharArray = MonoArray (type elem = CharArray.elem
                                 structure V = CharVector)

structure Int32Vector = MonoVector (type elem = Int32Vector.elem)
structure Int32Array = MonoArray (type elem = Int32Array.elem
                                  structure V = Int32Vector)

structure Int64Vector = MonoVector (type elem = Int64Vector.elem)
structure Int64Array = MonoArray (type elem = Int64Array.elem
                                  structure V = Int64Vector)

structure RealVector = MonoVector (type elem = RealVector.elem)
structure RealArray = MonoArray (type elem = RealArray.elem
                                 structure V = RealVector)

datatype mpitype =
         MPI_CHAR_t
       | MPI_INT_t 
       | MPI_LONG_t
       | MPI_REAL_t
       | MPI_CHAR_ARRAY_t
       | MPI_INT_ARRAY_t
       | MPI_LONG_ARRAY_t
       | MPI_REAL_ARRAY_t

datatype mpidata =
         MPI_CHAR of char
       | MPI_INT  of int
       | MPI_LONG of Int64.int
       | MPI_REAL of real
       | MPI_CHAR_ARRAY of CharArray.array
       | MPI_INT_ARRAY  of Int32Array.array
       | MPI_LONG_ARRAY of Int64Array.array
       | MPI_REAL_ARRAY of RealArray.array


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

 val Finalize   = _import "MPI_Finalize" : unit -> unit;

 val Wtime      = _import "MPI_Wtime": unit -> real ;

 val Barrier    = _import "MPI_Barrier" : comm -> int ;

 structure Utils =
 struct
 
 fun chunkSize (totalSize, nprocs, myrank) =
     if (nprocs < 1) orelse (nprocs <= myrank)
     then raise Overflow
     else (if totalSize < myrank
           then 0 
           else (if totalSize <= nprocs
                 then 1
                 else (let 
                          val chunkDiv = Int.div (totalSize, nprocs)
                          val chunkRem = totalSize - nprocs * chunkDiv 
                      in
                          if myrank < chunkRem then chunkDiv+1 else chunkDiv
                      end)))
 end

 fun gid (lid, nprocs, myrank) = myrank + nprocs * lid

 end
 
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


    val cSendChar = _import "mlton_MPI_Send_char" : CharArray.array * int * int * int * comm -> int;

    val cSendInt  = _import "mlton_MPI_Send_int"  : Int32Array.array * int * int * int * comm -> int;

    val cSendLong = _import "mlton_MPI_Send_long" : Int64Array.array * int * int * int * comm -> int;

    val cSendReal = _import "mlton_MPI_Send_double" : RealArray.array * int * int * int * comm -> int;

    fun Send (v, dest, tag, comm) =
        case v of
            MPI_CHAR c => cSendChar (CharArray.fromList [c], 1, dest, tag, comm)
          | MPI_CHAR_ARRAY a => cSendChar (a, CharArray.length a, dest, tag, comm)
          | MPI_INT i => cSendInt (Int32Array.fromList [i], 1, dest, tag, comm)
          | MPI_INT_ARRAY a => cSendInt (a, Int32Array.length a, dest, tag, comm)
          | MPI_LONG i => cSendLong (Int64Array.fromList [i], 1, dest, tag, comm)
          | MPI_LONG_ARRAY a => cSendLong (a, Int64Array.length a, dest, tag, comm)
          | MPI_REAL i => cSendReal (RealArray.fromList [i], 1, dest, tag, comm)
          | MPI_REAL_ARRAY a => cSendReal (a, RealArray.length a, dest, tag, comm)

    val cRecvChar = _import "mlton_MPI_Recv_char" : CharArray.array * int * int * int * comm -> int;

    val cRecvInt  = _import "mlton_MPI_Recv_int"  : Int32Array.array * int * int * int * comm -> int;

    val cRecvLong = _import "mlton_MPI_Recv_long" : Int64Array.array * int * int * int * comm -> int;

    val cRecvReal = _import "mlton_MPI_Recv_double" : RealArray.array * int * int * int * comm -> int;

    fun Recv (ty, source, tag, comm) =
        case ty of
            MPI_CHAR_t => 
            let
                val r = CharArray.fromList [Char.chr 0]
                val status = cRecvChar (r, 1, source, tag, comm)
            in
                if (not (status = 0))
                then raise MPIError (status, "Recv error")
                else MPI_CHAR (CharArray.sub(r,0))
            end
          | MPI_INT_t => 
            let
                val r = Int32Array.fromList [0]
                val status = cRecvInt (r, 1, source, tag, comm)
            in
                if (not (status = 0))
                then raise MPIError (status, "Recv error")
                else MPI_INT (Int32Array.sub(r,0))
            end
          | MPI_LONG_t => 
            let
                val r = Int64Array.fromList [0]
                val status = cRecvLong (r, 1, source, tag, comm)
            in
                if (not (status = 0))
                then raise MPIError (status, "Recv error")
                else MPI_LONG (Int64Array.sub(r,0))
            end
          | MPI_REAL_t => 
            let
                val r = RealArray.fromList [0.0]
                val status = cRecvReal (r, 1, source, tag, comm);
            in 
                if (not (status = 0))
                then raise MPIError (status, "Recv error")
                else MPI_REAL (RealArray.sub(r,0))
            end
          | MPI_CHAR_ARRAY_t => 
            let
                val (n,_,_) = Probe (source, tag, comm) 
                val a = CharArray.array (n, Char.chr 0)
                val status = cRecvChar (a, n, source, tag, comm)
            in 
                if (not (status = 0))
                then raise MPIError (status, "Recv error")
                else MPI_CHAR_ARRAY (a)
            end
          | MPI_INT_ARRAY_t => 
            let
                val (n,_,_) = Probe (source, tag, comm) 
                val a = Int32Array.array (n, 0)
                val status = cRecvInt (a, n, source, tag, comm)
            in
                if (not (status = 0))
                then raise MPIError (status, "Recv error")
                else MPI_INT_ARRAY (a)
            end
          | MPI_LONG_ARRAY_t => 
            let
                val (n,_,_) = Probe (source, tag, comm) 
                val a = Int64Array.array (n, 0)
                val status = cRecvLong (a, n, source, tag, comm)
            in
                if (not (status = 0))
                then raise MPIError (status, "Recv error")
                else MPI_LONG_ARRAY (a)
            end
          | MPI_REAL_ARRAY_t =>
            let
                val (n,_,_) = Probe (source, tag, comm) 
                val a = RealArray.array (n, 0.0)
                val status = cRecvReal (a, n, source, tag, comm)
            in
                if (not (status = 0))
                then raise MPIError (status, "Recv error")
                else MPI_REAL_ARRAY (a)
            end
 end

 structure Collective =
 struct


    val nullCharArray = CharArray.fromList []
    val nullIntArray  = Int32Array.fromList []
    val nullLongArray = Int64Array.fromList []
    val nullRealArray = RealArray.fromList []

    val cBcastChar = _import "mlton_MPI_Bcast_char" : CharArray.array * int * int * comm -> int;

    val cBcastInt = _import "mlton_MPI_Bcast_int" : Int32Array.array * int * int * comm -> int;

    val cBcastLong = _import "mlton_MPI_Bcast_long" : Int64Array.array * int * int * comm -> int;

    val cBcastReal = _import "mlton_MPI_Bcast_double" : RealArray.array * int * int * comm -> int;

    fun Bcast (v, root, comm) =
        case v of
            MPI_CHAR c => cBcastChar (CharArray.fromList [c], 1, root, comm)
          | MPI_CHAR_ARRAY a => cBcastChar (a, CharArray.length a, root, comm)
          | MPI_INT i => cBcastInt (Int32Array.fromList [i], 1, root, comm)
          | MPI_INT_ARRAY a => cBcastInt (a, Int32Array.length a, root, comm)
          | MPI_LONG i => cBcastLong (Int64Array.fromList [i], 1, root, comm)
          | MPI_LONG_ARRAY a => cBcastLong (a, Int64Array.length a, root, comm)
          | MPI_REAL i => cBcastReal (RealArray.fromList [i], 1, root, comm)
          | MPI_REAL_ARRAY a => cBcastReal (a, RealArray.length a, root, comm)

    val cScatterChar = _import "mlton_MPI_Scatter_char"   : CharArray.array * int * CharArray.array * int * int * comm -> int;

    val cScatterInt  = _import "mlton_MPI_Scatter_int"    : Int32Array.array * int * Int32Array.array * int * int * comm -> int;

    val cScatterLong = _import "mlton_MPI_Scatter_long"   : Int64Array.array * int * Int64Array.array * int * int * comm -> int;

    val cScatterReal = _import "mlton_MPI_Scatter_double" : RealArray.array * int * RealArray.array * int * int * comm -> int;


    exception InvalidScatter
    exception InvalidScatterDataSize of int * int * int

    fun sendScatter (s, sn, r, root, comm) =
        let 
            val nprocs = Comm.Size comm
        in
            case (s, r) of
                (MPI_CHAR_ARRAY s, MPI_CHAR_ARRAY r) => 
                let 
                    val rn    = CharArray.length r
                    val slen  = CharArray.length s
                in
                    if (nprocs * sn) <= slen
                    then (cScatterChar (s, sn, r, rn, root, comm); 
                          if rn = 1 then (MPI_CHAR (CharArray.sub (r,0))) else (MPI_CHAR_ARRAY r))
                    else raise InvalidScatterDataSize (nprocs, sn, slen)
                end
              | (MPI_INT_ARRAY s, MPI_INT_ARRAY r) =>
                let 
                    val rn = Int32Array.length r
                    val slen  = Int32Array.length s
                in
                    if (nprocs * sn) <= slen
                    then (cScatterInt (s, sn, r, rn, root, comm); 
                          if rn = 1 then (MPI_INT (Int32Array.sub (r,0))) else (MPI_INT_ARRAY r))
                    else raise InvalidScatterDataSize (nprocs, sn, slen)
                end
              | (MPI_LONG_ARRAY s, MPI_LONG_ARRAY r) =>
                let 
                    val rn = Int64Array.length r
                    val slen = Int64Array.length s
                in
                    if (nprocs * sn) <= slen
                    then (cScatterLong (s, sn, r, rn, root, comm); 
                          if rn = 1 then (MPI_LONG (Int64Array.sub (r,0))) else (MPI_LONG_ARRAY r))
                    else raise InvalidScatterDataSize (nprocs, sn, slen)
                end
              | (MPI_REAL_ARRAY s, MPI_REAL_ARRAY r) =>
                let 
                    val rn = RealArray.length r
                    val slen = RealArray.length s
                in
                    if (nprocs * sn) <= slen
                    then (cScatterReal (s, sn, r, rn, root, comm); 
                          if rn = 1 then (MPI_REAL (RealArray.sub (r,0))) else (MPI_REAL_ARRAY r))
                    else raise InvalidScatterDataSize (nprocs, sn, slen)
                end
                | (_, _) => raise InvalidScatter
        end


    fun recvScatter (r, root, comm) =
        let 
            val nprocs = Comm.Size comm
        in
            case r of
                MPI_CHAR_ARRAY r => 
                let 
                    val rn = CharArray.length r
                in
                    (cScatterChar (nullCharArray, 0, r, rn, root, comm); 
                     if rn = 1 then (MPI_CHAR (CharArray.sub (r,0))) else (MPI_CHAR_ARRAY r))
                end
              | MPI_INT_ARRAY r => 
                let 
                    val rn = Int32Array.length r
                in
                    (cScatterInt (nullIntArray, 0, r, rn, root, comm); 
                     if rn = 1 then (MPI_INT (Int32Array.sub (r,0))) else (MPI_INT_ARRAY r))
                end
              | MPI_LONG_ARRAY r => 
                let 
                    val rn = Int64Array.length r
                in
                    (cScatterLong (nullLongArray, 0, r, rn, root, comm); 
                  if rn = 1 then (MPI_LONG (Int64Array.sub (r,0))) else (MPI_LONG_ARRAY r))
                end
              | MPI_REAL_ARRAY r => 
                let 
                    val rn = RealArray.length r
                in
                    (cScatterReal (nullRealArray, 0, r, rn, root, comm); 
                     if rn = 1 then (MPI_REAL (RealArray.sub (r,0))) else (MPI_REAL_ARRAY r))
                end
                | _ => raise InvalidScatter

        end

    val cRecvScattervChar = _import "mlton_MPI_RecvScatterv_char"   : CharArray.array * int * int * comm -> int ;
    val cSendScattervChar = _import "mlton_MPI_SendScatterv_char"   : CharArray.array * Int32Array.array  * Int32Array.array *
                                                                      CharArray.array * int * int * comm -> int ;

    val cRecvScattervInt = _import "mlton_MPI_RecvScatterv_int"   : Int32Array.array * int * int * comm -> int ;
    val cSendScattervInt = _import "mlton_MPI_SendScatterv_int"   : Int32Array.array * Int32Array.array  * Int32Array.array *
                                                                    Int32Array.array * int * int * comm -> int ;
        
    val cRecvScattervLong = _import "mlton_MPI_RecvScatterv_long"   : Int64Array.array * int * int * comm -> int ;
    val cSendScattervLong = _import "mlton_MPI_SendScatterv_long"   : Int64Array.array * Int32Array.array  * Int32Array.array *
                                                                      Int64Array.array * int * int * comm -> int ;
        
    val cRecvScattervReal = _import "mlton_MPI_RecvScatterv_double"   : RealArray.array * int * int * comm -> int ;
    val cSendScattervReal = _import "mlton_MPI_SendScatterv_double"   : RealArray.array * Int32Array.array  * Int32Array.array *
                                                                        RealArray.array * int * int * comm -> int ;
        
        
    exception InvalidScatterv
  
              
    fun sendScatterv (lst, root, comm) =
      let 
          val nprocs = Comm.Size comm

          val rootfn = fn (lst, makeArray, scatterArray, ret) =>
                          (let 
                               val sendlengths = List.map Array.length lst
                               val (_,displs)  = List.foldr (fn (len,(i,lst)) => (i+len,i :: lst)) (0,[]) sendlengths
                               val sendlenarray = Int32Array.fromList sendlengths
                                                  
                               (* Scatters the lengths of the buffers to all the processes *)
                               val mylen = (case sendScatter (MPI_INT_ARRAY sendlenarray, 1,
                                                              MPI_INT_ARRAY (Int32Array.array (1,~1)), 
                                                              root, comm) of
                                                MPI_INT n => n
                                               | _ => raise InvalidScatterv)
                                           
                               val total = List.foldl (op +) 0 sendlengths
                                           
                               val sendbuf = makeArray total
                                             
                               (* Builds a single buffer with all data *)
                               val _ = ListPair.app (fn (a,offset) => Array.copy {di=offset,dst=sendbuf,src=a})
                                                    (lst, displs)
                                       
		               (* Allocates receive buffer *)
                               val myrecv = makeArray mylen
                                            
                           in
		               (* Performs the scatter & returns received value *)
                               scatterArray (sendbuf, sendlenarray, Int32Array.fromList displs, 
                                             myrecv, mylen, root, comm);
                               ret myrecv
                            end)
                          
      in
          case hd lst of
              MPI_CHAR_ARRAY _ => 
	      (let 
                   val ndata = List.length lst
                   val lst'  = List.map (fn (MPI_CHAR_ARRAY a) => a | _ => raise InvalidScatterv) lst
               in
	           if (not (ndata = nprocs))
                   then raise InvalidScatterDataSize (nprocs, ndata, 0)
                   else rootfn (List.map CharArray.toPoly lst', fn (s) => Array.array (s, Char.chr 0),
                                fn (sendbuf, sendlen, displs, myrecv, mylen, root, comm) =>
                                   cSendScattervChar (CharArray.fromPoly sendbuf, sendlen, displs,
                                                      CharArray.fromPoly myrecv, mylen, root, comm),
                                MPI_CHAR_ARRAY)
               end)

            | MPI_INT_ARRAY _ => 
	      (let 
                   val ndata = List.length lst
                   val lst'  = List.map (fn (MPI_INT_ARRAY a) => a | _ => raise InvalidScatterv) lst
               in
	           if (not (ndata = nprocs))
                   then raise InvalidScatterDataSize (nprocs, ndata, 0)
                   else rootfn (List.map Int32Array.toPoly lst', fn (s) => Array.array (s, ~1),
                                fn (sendbuf, sendlen, displs, myrecv, mylen, root, comm) =>
                                   cSendScattervInt (Int32Array.fromPoly sendbuf, sendlen, displs,
                                                     Int32Array.fromPoly myrecv, mylen, root, comm),
                               MPI_INT_ARRAY)
               end)

            | MPI_LONG_ARRAY _ => 
	      (let 
                   val ndata = List.length lst
                   val lst'  = List.map (fn (MPI_LONG_ARRAY a) => a | _ => raise InvalidScatterv) lst
               in
	           if (not (ndata = nprocs))
                   then raise InvalidScatterDataSize (nprocs, ndata, 0)
                   else rootfn (List.map Int64Array.toPoly lst', fn (s) => Array.array (s, ~1),
                                fn (sendbuf, sendlen, displs, myrecv, mylen, root, comm) =>
                                   cSendScattervLong (Int64Array.fromPoly sendbuf, sendlen, displs,
                                                     Int64Array.fromPoly myrecv, mylen, root, comm),
                               MPI_LONG_ARRAY)
                                
               end)

            | MPI_REAL_ARRAY _ => 
	      (let 
                   val ndata = List.length lst
                   val lst'  = List.map (fn (MPI_REAL_ARRAY a) => a | _ => raise InvalidScatterv) lst
               in
	           if (not (ndata = nprocs))
                   then raise InvalidScatterDataSize (nprocs, ndata, 0)
                   else rootfn (List.map RealArray.toPoly lst', fn (s) => Array.array (s, ~1.0),
                                fn (sendbuf, sendlen, displs, myrecv, mylen, root, comm) =>
                                   cSendScattervReal (RealArray.fromPoly sendbuf, sendlen, displs,
                                                      RealArray.fromPoly myrecv, mylen, root, comm),
                               MPI_REAL_ARRAY)
               end)
            
            | _ => raise InvalidScatterv

      end

              
    fun recvScatterv (ty, root, comm) =
      let 
          val nprocs = Comm.Size comm

          val nonrootfn = fn (makeArray, scatterArray, ret) =>
                             (let 
	                          (* If not root, get our length *)
                                  val mylen = (case recvScatter (MPI_INT_ARRAY (Int32Array.array (1,~1)), root, comm) of
                                                   MPI_INT i => i | _ => raise InvalidScatterv)

	                          (* Allocates receive buffer *)
                                  val myrecv = makeArray mylen
                              in
	                          (* Performs the scatter & returns received value *)
	                          scatterArray (myrecv, mylen, root, comm);
	                          ret myrecv
                              end)
                           
      in
          case ty of

              MPI_CHAR_ARRAY_t => 
	      nonrootfn (fn (s) => Array.array (s, Char.chr 0),
                         fn (myrecv, mylen, root, comm) =>
                            cRecvScattervChar (CharArray.fromPoly myrecv, mylen, root, comm),
                         MPI_CHAR_ARRAY)

            | MPI_INT_ARRAY_t => 
	      nonrootfn (fn (s) => Array.array (s, ~1),
                         fn (myrecv, mylen, root, comm) =>
                            cRecvScattervInt (Int32Array.fromPoly myrecv, mylen, root, comm),
                         MPI_INT_ARRAY)

            | MPI_LONG_ARRAY_t => 
	      nonrootfn (fn (s) => Array.array (s, ~1),
                         fn (myrecv, mylen, root, comm) =>
                            cRecvScattervLong (Int64Array.fromPoly myrecv, mylen, root, comm),
                         MPI_LONG_ARRAY)
                         
            | MPI_REAL_ARRAY_t => 
	      nonrootfn (fn (s) => RealArray.array (s, ~1.0),
                         fn (myrecv, mylen, root, comm) =>
                            cRecvScattervReal (RealArray.fromPoly myrecv, mylen, root, comm),
                         MPI_REAL_ARRAY)
            | _ => raise InvalidScatterv
      end


    val cGatherChar = _import "mlton_MPI_Gather_char"   : CharArray.array * int * CharArray.array * int * int * comm -> int;

    val cGatherInt  = _import "mlton_MPI_Gather_int"    : Int32Array.array * int * Int32Array.array * int * int * comm -> int;

    val cGatherLong = _import "mlton_MPI_Gather_long"   : Int64Array.array * int * Int64Array.array * int * int * comm -> int;

    val cGatherReal = _import "mlton_MPI_Gather_double" : RealArray.array * int * RealArray.array * int * int * comm -> int;

    exception InvalidGather
    exception InvalidGatherDataSize of int * int * int

    fun recvGather (r, rn, s, root, comm) =
        let 
            val nprocs = Comm.Size comm
        in
            case (r, s) of
                (MPI_CHAR_ARRAY r, MPI_CHAR_ARRAY s) => 
                let 
                    val sn    = CharArray.length s
                    val rlen  = CharArray.length r
                in
                    if (nprocs * rn) <= rlen
                    then (cGatherChar (s, sn, r, rn, root, comm); 
                           (MPI_CHAR_ARRAY r))
                    else raise InvalidGatherDataSize (nprocs, rn, rlen)
                end
              | (MPI_INT_ARRAY r, MPI_INT_ARRAY s) => 
                let 
                    val sn = Int32Array.length s
                    val rlen  = Int32Array.length r
                in
                    if (nprocs * rn) <= rlen
                    then (cGatherInt (s, sn, r, rn, root, comm); 
                          (MPI_INT_ARRAY r))
                    else raise InvalidGatherDataSize (nprocs, rn, rlen)
                end
              | (MPI_LONG_ARRAY r, MPI_LONG_ARRAY s) => 
                let 
                    val sn = Int64Array.length s
                    val rlen = Int64Array.length r
                in
                    if (nprocs * rn) <= rlen
                    then (cGatherLong (s, sn, r, rn, root, comm); 
                          (MPI_LONG_ARRAY r))
                    else raise InvalidGatherDataSize (nprocs, rn, rlen)
                end
              | (MPI_REAL_ARRAY r, MPI_REAL_ARRAY s) =>
                let 
                    val sn = RealArray.length s
                    val rlen = RealArray.length r
                in
                    if (nprocs * rn) <= rlen
                    then (cGatherReal (s, sn, r, rn, root, comm); 
                          (MPI_REAL_ARRAY r))
                    else raise InvalidScatterDataSize (nprocs, rn, rlen)
                end
              | (_, _) => raise InvalidGather

    end

    fun sendGather (s, root, comm) =
        let 
            val nprocs = Comm.Size comm
        in
            case s of
                MPI_CHAR_ARRAY s => 
                let 
                    val sn = CharArray.length s
                in
                    cGatherChar (s, sn, nullCharArray, 0, root, comm)
                end
              | MPI_INT_ARRAY s => 
                let 
                    val sn = Int32Array.length s
                in
                    cGatherInt (s, sn, nullIntArray, 0, root, comm)
                end
              | MPI_LONG_ARRAY s => 
                let 
                    val sn = Int64Array.length s
                in
                    cGatherLong (s, sn, nullLongArray, 0, root, comm)
                end
              | MPI_REAL_ARRAY s =>
                let 
                    val sn = RealArray.length s
                in
                    cGatherReal (s, sn, nullRealArray, 0, root, comm)
                end
              | _ => raise InvalidGather
    end


    val cSendGathervChar = _import "mlton_MPI_SendGatherv_char"   : CharArray.array * int * int * comm -> int;
    val cRecvGathervChar = _import "mlton_MPI_RecvGatherv_char"   : CharArray.array * int * CharArray.array * Int32Array.array * Int32Array.array * int * comm -> int;
            
    val cSendGathervInt = _import "mlton_MPI_SendGatherv_int"   : Int32Array.array * int * int * comm -> int;
    val cRecvGathervInt = _import "mlton_MPI_RecvGatherv_int"   : Int32Array.array * int * Int32Array.array * Int32Array.array * Int32Array.array * int * comm -> int;

    val cSendGathervLong = _import "mlton_MPI_SendGatherv_long"   : Int64Array.array * int * int * comm -> int;
    val cRecvGathervLong = _import "mlton_MPI_RecvGatherv_long"   : Int64Array.array * int * Int64Array.array * Int32Array.array * Int32Array.array * int * comm -> int;

    val cSendGathervReal = _import "mlton_MPI_SendGatherv_double"   : RealArray.array * int * int * comm -> int;
    val cRecvGathervReal = _import "mlton_MPI_RecvGatherv_double"   : RealArray.array * int * RealArray.array * Int32Array.array * Int32Array.array * int * comm -> int;
            
    exception InvalidGatherv
  
              
    fun recvGatherv (s, root, comm) =
      let 
          val nprocs = Comm.Size comm

          val rootfn = fn (sendbuf, makeArray, gatherArray, ret) =>
                          (let 
                              val mylen = Array.length sendbuf

                              (* Gather the lengths of the buffers from all processes *)
                              val recvlengths = case recvGather (MPI_INT_ARRAY (Int32Array.array (nprocs, ~1)),
                                                                 1, MPI_INT_ARRAY (Int32Array.array (1,mylen)),
                                                                 root, comm) of
                                                    (MPI_INT_ARRAY a) => a
                                                  | _ => raise InvalidGatherv
                                                    
                                                    
                              val displs  = List.rev (#2(Int32Array.foldl (fn (len,(i,lst)) => (i+len,i :: lst)) (0,[]) recvlengths))
     
                              val total = Int32Array.foldl (op +) 0 recvlengths
                                                     
		              (* Allocates receive buffer *)
                              val recvbuf = makeArray total

                              (* Gather the data *)
                              val _ = gatherArray (sendbuf, mylen, 
                                                   recvbuf, recvlengths, Int32Array.fromList displs, 
                                                   root, comm);
                          in
                              (ret recvbuf, recvlengths)
                          end)
                           
      in
          case s of
              MPI_CHAR_ARRAY s =>
              rootfn (s, fn (s) => Array.array (s, Char.chr 0),
                      fn (sendbuf, sendlen, recvbuf, recvlen, displs, root, comm) =>
                         cRecvGathervChar (CharArray.fromPoly sendbuf, sendlen, 
                                           CharArray.fromPoly recvbuf, recvlen, displs, 
                                           root, comm),
                      MPI_CHAR_ARRAY)

            | MPI_INT_ARRAY s => 
              rootfn (s, fn (s) => Array.array (s, ~1),
                      fn (sendbuf, sendlen, recvbuf, recvlen, displs, root, comm) =>
                         cRecvGathervInt (Int32Array.fromPoly sendbuf, sendlen, 
                                          Int32Array.fromPoly recvbuf, recvlen, displs,
                                          root, comm),
                      MPI_INT_ARRAY)

            | MPI_LONG_ARRAY s =>
              rootfn (s, fn (s) => Array.array (s, ~1),
                      fn (sendbuf, sendlen, recvbuf, recvlen, displs, root, comm) =>
                         cRecvGathervLong (Int64Array.fromPoly sendbuf, sendlen, 
                                           Int64Array.fromPoly recvbuf, recvlen, displs,
                                           root, comm),
                      MPI_LONG_ARRAY)
                                
            | MPI_REAL_ARRAY s =>
              rootfn (s, fn (s) => Array.array (s, ~1.0),
                      fn (sendbuf, sendlen, recvbuf, recvlen, displs, root, comm) =>
                         cRecvGathervReal (RealArray.fromPoly sendbuf, sendlen, 
                                           RealArray.fromPoly recvbuf, recvlen, displs,
                                           root, comm),
                      MPI_REAL_ARRAY)

            | _ => raise InvalidGatherv

      end

              
    fun sendGatherv (s, root, comm) =
      let 
          val nprocs = Comm.Size comm
                          
          val nonrootfn = fn (sendbuf, makeArray, gatherArray) =>
                             (let 
	                          (* If not root, get our length and send it to root *)
                                 val mylen = Array.length sendbuf
                                                          
                                 val _ = sendGather (MPI_INT_ARRAY (Int32Array.array (1,mylen)), root, comm)
                              in
	                          (* Performs the gather *)
	                          gatherArray (sendbuf, mylen, root, comm)
                              end)
                           
      in
          case s of
              MPI_CHAR_ARRAY s => 
	      nonrootfn (s, fn (s) => Array.array (s, Char.chr 0),
                         fn (sendbuf, sendlen, root, comm) =>
                            cSendGathervChar (CharArray.fromPoly sendbuf, sendlen, root, comm))

            | MPI_INT_ARRAY s =>
	      nonrootfn (s, fn (s) => Array.array (s, ~1),
                         fn (sendbuf, sendlen, root, comm) =>
                            cSendGathervInt (Int32Array.fromPoly sendbuf, sendlen, root, comm))

            | MPI_LONG_ARRAY s =>
	      nonrootfn (s, fn (s) => Array.array (s, ~1),
                         fn (sendbuf, sendlen, root, comm) =>
                            cSendGathervLong (Int64Array.fromPoly sendbuf, sendlen, root, comm))

            | MPI_REAL_ARRAY s =>
	      nonrootfn (s, fn (s) => RealArray.array (s, ~1.0),
                         fn (sendbuf, sendlen, root, comm) =>
                            cSendGathervReal (RealArray.fromPoly sendbuf, sendlen, root, comm))

            | _ => raise InvalidGatherv
      end
 end
end




