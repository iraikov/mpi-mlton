#include <stdio.h>
#include <mpi.h>
#include "mpiexport.h"

typedef int rank_t;

static void mlton_MPI_error_handler (MPI_Comm * comm, int * errcode, ...)
{
  char errmsg[MPI_MAX_ERROR_STRING + 1];
  int resultlen;
  
  MPI_Error_string(*errcode, errmsg, &resultlen);
  errmsg[resultlen-1] = 0;
  fprintf(stderr, "%s\n", errmsg);

  mlton_MPI_exception (*errcode, resultlen, errmsg);
}



void mlton_MPI_Init (Int32 argc, Pointer argv, Pointer argv_lens)
{

  int i, cargvsz, slen;
  char *s, *x, **cargv;
  MPI_Errhandler hdlr;

  cargvsz = (argc+1) * sizeof(char *);
  if ((cargv = malloc(cargvsz)) != NULL)
    {
      if (argc > 0)
        {
          for (i = 0; i < argc; i++) 
            {
              x = ((char **)argv)[i];
              slen = ((int *)argv_lens)[i];
              if (( s = malloc (slen+1)) != NULL)
                {
                  memcpy (s, (char *)x, slen);
                  s[slen] = 0;
                  cargv[i] = s;
                } else
                { 
                  cargv[i] = NULL;
                }
              x += slen;
            }
        } else
        {
          i = 0;
        }
      
      cargv[i] = NULL;
      
      MPI_Init(&argc, &cargv);
      
      for (i = 0; i < argc; i++)
        {  
          s = cargv[i];
	  if (s != NULL)
            {
              free (s);
            }
        }
      memset (cargv, (int)NULL, cargvsz);
      free (cargv);
    }
  
  MPI_Errhandler_create((MPI_Handler_function *)mlton_MPI_error_handler, &hdlr);
  MPI_Errhandler_set(MPI_COMM_WORLD, hdlr);
}


void *mlton_MPI_comm_world (void)
{
  return MPI_COMM_WORLD;
}


int mlton_MPI_get_any_tag(void)
{
  return MPI_ANY_TAG;
}


int mlton_MPI_get_any_source (void)
{
  return (MPI_ANY_SOURCE);
}



void mlton_MPI_probe (int *status_count, int *status_source, int *status_tag,
                     int source, int tag, MPI_Comm comm)
{
  MPI_Status status;
  
  MPI_Probe(source, tag, comm, &status);
  MPI_Get_count(&status, MPI_UINT8_T, status_count);

  *status_source = status.MPI_SOURCE;

  *status_tag = status.MPI_TAG;
}


int mlton_MPI_Recv_Word8 (uint8_t *v, size_t n, int source, int tag, MPI_Comm comm)
{
   return MPI_Recv(v, n, MPI_UINT8_T, source, tag, comm, MPI_STATUS_IGNORE);
}


int mlton_MPI_Send_Word8 (uint8_t *v, size_t n, int dest, int tag, MPI_Comm comm)
{
  int status;
  status = MPI_Send(v, n, MPI_UINT8_T, dest, tag, comm);
  return status;
}


int mlton_MPI_Bcast_Word8 (uint8_t *v, size_t n, uint8_t *out, int root, MPI_Comm comm)
{
  int status = 0;
  rank_t myrank;
  MPI_Comm_rank(comm, &myrank);
  if (myrank == root)
    {
      memcpy(out, v, sizeof(*v)*n);
    }
  status = MPI_Bcast(out, n, MPI_UINT8_T, root, comm);
  return status;
}


int mlton_MPI_Scatter_Word8 (uint8_t *sendbuf, int sendcount, 
                             uint8_t *recvbuf, int nrecv, int root, MPI_Comm comm)
{
  int result;

  result = MPI_Scatter (sendbuf, sendcount, MPI_UINT8_T,
                        recvbuf, nrecv, MPI_UINT8_T,
                        root, comm);

  return result;
   
}


int mlton_MPI_Scatterv_Word8 (uint8_t *sendbuf, int *sendcounts, int *displs, 
                               uint8_t *recvbuf, int nrecv, int root, MPI_Comm comm)
{
  int result;

  result = MPI_Scatterv (sendbuf, sendcounts, displs, MPI_UINT8_T,
                         recvbuf, nrecv, MPI_UINT8_T,
                         root, comm);

  return result;
   
}

int mlton_MPI_Gather_Word8 (uint8_t *sendbuf, size_t sn, uint8_t *recvbuf, 
                            int recvcount, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gather(sendbuf, sn, MPI_UINT8_T, recvbuf, recvcount, MPI_UINT8_T, root, comm);

  return result;
}


int mlton_MPI_Gatherv_Word8 (uint8_t *sendbuf, size_t sn, uint8_t *recvbuf, 
                              int *recvcounts, int *displs, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_UINT8_T, recvbuf, recvcounts, displs, MPI_UINT8_T, root, comm);

  return result;
}


int mlton_MPI_Allgather_Word8 (uint8_t *sendbuf, size_t sn, uint8_t *recvbuf, 
                                int recvcount, MPI_Comm comm)
{
  int result; 

  result = MPI_Allgather(sendbuf, sn, MPI_UINT8_T, recvbuf, recvcount, MPI_UINT8_T, comm);

  return result;
}


int mlton_MPI_Allgatherv_Word8 (uint8_t *sendbuf, size_t sn, uint8_t *recvbuf, 
                                int *recvcounts, int *displs, MPI_Comm comm)
{
  int result; 

  result = MPI_Allgatherv(sendbuf, sn, MPI_UINT8_T, recvbuf, recvcounts, displs, MPI_UINT8_T, comm);

  return result;
}


int mlton_MPI_Alltoall_Word8 (uint8_t *sendbuf, size_t sn, uint8_t *recvbuf, 
                              int recvcount, MPI_Comm comm)
{
  int result; 

  result = MPI_Alltoall(sendbuf, sn, MPI_UINT8_T, recvbuf, recvcount, MPI_UINT8_T, comm);

  return result;
}



int mlton_MPI_Alltoallv_Word8 (uint8_t *sendbuf, int *sendcounts, int *sdispls,
                               uint8_t *recvbuf, int *recvcounts, int *rdispls, MPI_Comm comm)
{
  int result; 

  result = MPI_Alltoallv(sendbuf, sendcounts, sdispls, MPI_UINT8_T, recvbuf, recvcounts, rdispls, MPI_UINT8_T, comm);

  return result;
}
