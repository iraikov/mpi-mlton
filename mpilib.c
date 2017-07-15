#include <stdio.h>
#include <mpi.h>
#include "mpiexport.h"

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
  MPI_Get_count(&status, MPI_UINT32, status_count);

  *status_source = status.MPI_SOURCE;

  *status_tag = status.MPI_TAG;
}


int mlton_MPI_Recv_Word32 (uint32_t *v, size_t n, int source, int tag, MPI_Comm comm)
{
   return MPI_Recv(v, n, MPI_UINT32, source, tag, comm, MPI_STATUS_IGNORE);
}


int mlton_MPI_Send_Word32 (uint32_t *v, size_t n, int dest, int tag, MPI_Comm comm)
{
  int status;
  status = MPI_Send(v, n, MPI_UINT32, dest, tag, comm);
  return status;
}


int mlton_MPI_Bcast_Word32 (uint32_t *v, size_t n, int root, MPI_Comm comm)
{
  return MPI_Bcast(v, n, MPI_UINT32, root, comm);
}


int mlton_MPI_Scatterv_Word32 (uint32_t *sendbuf, int *sendcounts, int *displs, 
                               uint32_t *recvbuf, int nrecv, int root, MPI_Comm comm)
{
  int result;

  result = MPI_Scatterv (sendbuf, sendcounts, displs, MPI_UINT32,
                         recvbuf, nrecv, MPI_UINT32,
                         root, comm);

  return result;
   
}

int mlton_MPI_Gatherv_Word32 (uint32_t *sendbuf, size_t sn, uint32_t *recvbuf, 
                              int *recvcounts, int *displs, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_UINT32, recvbuf, recvcounts, displs, MPI_UINT32, root, comm);

  return result;
}

