#include <mpi.h>
#include "mpiexport.h"

static void mlton_MPI_error_handler (MPI_Comm * comm, int * errcode, ...)
{
  char errmsg[MPI_MAX_ERROR_STRING + 1];
  int resultlen;

  MPI_Error_string(*errcode, errmsg, &resultlen);
  errmsg[resultlen-1] = 0;

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
  MPI_Get_count(&status, MPI_BYTE, status_count);

  *status_source = status.MPI_SOURCE;

  *status_tag = status.MPI_TAG;
}


int mlton_MPI_Recv_char (char *v, size_t n, int source, int tag, MPI_Comm comm)
{
   return MPI_Recv(v, n, MPI_CHAR, source, tag, comm, MPI_STATUS_IGNORE);
}


int mlton_MPI_Recv_int (int *v, size_t n, int source, int tag, MPI_Comm comm)
{
   return MPI_Recv(v, n, MPI_INT, source, tag, comm, MPI_STATUS_IGNORE);
}


int mlton_MPI_Recv_long (long int *v, size_t n, int source, int tag, MPI_Comm comm)
{
   return MPI_Recv(v, n, MPI_LONG, source, tag, comm, MPI_STATUS_IGNORE);
}


int mlton_MPI_Recv_double (double *v, size_t n, int source, int tag, MPI_Comm comm)
{
   return MPI_Recv(v, n, MPI_DOUBLE, source, tag, comm, MPI_STATUS_IGNORE);
}


int mlton_MPI_Send_char (char *v, size_t n, int dest, int tag, MPI_Comm comm)
{
  int status;
  status = MPI_Send(v, n, MPI_CHAR, dest, tag, comm);
  return status;
}


int mlton_MPI_Send_int (int *v, size_t n, int dest, int tag, MPI_Comm comm)
{
  return MPI_Send(v, n, MPI_INT, dest, tag, comm);
}


int mlton_MPI_Send_long (long int *v, size_t n, int dest, int tag, MPI_Comm comm)
{
  return MPI_Send(v, n, MPI_LONG, dest, tag, comm);
}


int mlton_MPI_Send_double (double *v, size_t n, int dest, int tag, MPI_Comm comm)
{
  return MPI_Send(v, n, MPI_DOUBLE, dest, tag, comm);
}


int mlton_MPI_Bcast_char (char *v, size_t n, int root, MPI_Comm comm)
{
  return MPI_Bcast(v, n, MPI_CHAR, root, comm);
}


int mlton_MPI_Bcast_int (int *v, size_t n, int root, MPI_Comm comm)
{
  return MPI_Bcast(v, n, MPI_INT, root, comm);
}


int mlton_MPI_Bcast_long (long int *v, size_t n, int root, MPI_Comm comm)
{
  return MPI_Bcast(v, n, MPI_LONG, root, comm);
}


int mlton_MPI_Bcast_double (double *v, size_t n, int root, MPI_Comm comm)
{
  return MPI_Bcast(v, n, MPI_DOUBLE, root, comm);
}


int mlton_MPI_Scatter_char (char *v, size_t n, char *rv, size_t rn, int root, MPI_Comm comm)
{
  int result; 

  if (n == 0)
  {
    result = MPI_Scatter(NULL, 0, MPI_DATATYPE_NULL, rv, rn, MPI_CHAR, root, comm);
  }
  else
  {
    result = MPI_Scatter(v, n, MPI_CHAR, rv, rn, MPI_CHAR, root, comm);
  }

  return result;
}


int mlton_MPI_Scatter_int (int *v, size_t n, int *rv, size_t rn, int root, MPI_Comm comm)
{
  int result; 

  if (n == 0)
  {
    result = MPI_Scatter(NULL, 0, MPI_DATATYPE_NULL, rv, rn, MPI_INT, root, comm);
  }
  else
  {
    result = MPI_Scatter(v, n, MPI_INT, rv, rn, MPI_INT, root, comm);
  }

  return result;
}

int mlton_MPI_Scatter_long (long int *v, size_t n, long int *rv, size_t rn, int root, MPI_Comm comm)
{
  int result; 

  if (n == 0)
  {
    result = MPI_Scatter(NULL, 0, MPI_DATATYPE_NULL, rv, rn, MPI_LONG, root, comm);
  }
  else
  {
    result = MPI_Scatter(v, n, MPI_LONG, rv, rn, MPI_LONG, root, comm);
  }

  return result;
}


int mlton_MPI_Scatter_double (double *v, size_t n, double *rv, size_t rn, int root, MPI_Comm comm)
{
  int result; 

  if (n == 0)
  {
    result = MPI_Scatter(NULL, 0, MPI_DATATYPE_NULL, rv, rn, MPI_DOUBLE, root, comm);
  }
  else
  {
    result = MPI_Scatter(v, n, MPI_DOUBLE, rv, rn, MPI_DOUBLE, root, comm);
  }

  return result;
}



int mlton_MPI_RecvScatterv_char (char *recvbuf, int nrecv,
                                     int root, MPI_Comm comm)
{
  int result;

  result = MPI_Scatterv (NULL, NULL, NULL, MPI_DATATYPE_NULL,
                         recvbuf, nrecv, MPI_CHAR,
                         root, comm);
  return result;
   
}


int mlton_MPI_SendScatterv_char (char *sendbuf, int *sendcounts, int *displs, 
                                  char *recvbuf, int nrecv,
                                  int root, MPI_Comm comm)
{
  int result;
  int size;

  result = MPI_Scatterv (sendbuf, sendcounts, displs, MPI_CHAR,
                         recvbuf, recvbuf, MPI_CHAR,
                         root, comm);

  return result;
   
}


int mlton_MPI_SendScatterv_int (int *sendbuf, int *sendcounts, int *displs, 
                                  int *recvbuf, int nrecv,
                                  int root, MPI_Comm comm)
{
  int result;
  int size;

  result = MPI_Scatterv (sendbuf, sendcounts, displs, MPI_INT,
                         recvbuf, recvbuf, MPI_INT,
                         root, comm);

  return result;
   
}

int mlton_MPI_RecvScatterv_int (int *recvbuf, int nrecv,
                                int root, MPI_Comm comm)
{
  int result;

  result = MPI_Scatterv (NULL, NULL, NULL, MPI_DATATYPE_NULL,
                         recvbuf, nrecv, MPI_INT,
                         root, comm);
  return result;
   
}


int mlton_MPI_SendScatterv_long (long int *sendbuf, int *sendcounts, int *displs, 
                                  long int *recvbuf, int nrecv,
                                  int root, MPI_Comm comm)
{
  int result;
  int size;

  result = MPI_Scatterv (sendbuf, sendcounts, displs, MPI_LONG,
                         recvbuf, recvbuf, MPI_LONG,
                         root, comm);

  return result;
   
}


int mlton_MPI_RecvScatterv_long (long int *recvbuf, int nrecv,
                                 int root, MPI_Comm comm)
{
  int result;

  result = MPI_Scatterv (NULL, NULL, NULL, MPI_DATATYPE_NULL,
                         recvbuf, nrecv, MPI_LONG,
                         root, comm);
  return result;
   
}


int mlton_MPI_RecvScatterv_double (double *sendbuf, int *sendcounts, int *displs, 
                                    double *recvbuf, int nrecv,
                                    int root, MPI_Comm comm)
{
  int result;
  int size;

  result = MPI_Scatterv (sendbuf, sendcounts, displs, MPI_DOUBLE,
                         recvbuf, recvbuf, MPI_DOUBLE,
                         root, comm);

  return result;
   
}


int mlton_MPI_SendScatterv_double (double *recvbuf, int nrecv,
                                   int root, MPI_Comm comm)
{
  int result;

  result = MPI_Scatterv (NULL, NULL, NULL, MPI_DATATYPE_NULL,
                         recvbuf, nrecv, MPI_DOUBLE,
                         root, comm);
  return result;
   
}


int mlton_MPI_Gather_char (char *v, size_t n, char *rv, size_t rn, int root, MPI_Comm comm)
{
  int result; 

  if (rn == 0)
  {
    result = MPI_Gather(v, n, MPI_CHAR, NULL, 0, MPI_DATATYPE_NULL, root, comm);
  }
  else
  {
    result = MPI_Gather(v, n, MPI_CHAR, rv, rn, MPI_CHAR, root, comm);
  }

  return result;
}


int mlton_MPI_Gather_int (int *v, size_t n, int *rv, size_t rn, int root, MPI_Comm comm)
{
  int result; 

  if (rn == 0)
  {
    result = MPI_Gather(v, n, MPI_INT, NULL, 0, MPI_DATATYPE_NULL, root, comm);
  }
  else
  {
    result = MPI_Gather(v, n, MPI_INT, rv, rn, MPI_INT, root, comm);
  }

  return result;
}


int mlton_MPI_Gather_long (long int *v, size_t n, long int *rv, size_t rn, int root, MPI_Comm comm)
{
  int result; 

  if (rn == 0)
  {
    result = MPI_Gather(v, n, MPI_LONG, NULL, 0, MPI_DATATYPE_NULL, root, comm);
  }
  else
  {
    result = MPI_Gather(v, n, MPI_LONG, rv, rn, MPI_LONG, root, comm);
  }

  return result;
}



int mlton_MPI_Gather_double (double *v, size_t n, double *rv, size_t rn, int root, MPI_Comm comm)
{
  int result; 

  if (rn == 0)
  {
    result = MPI_Gather(v, n, MPI_DOUBLE, NULL, 0, MPI_DATATYPE_NULL, root, comm);
  }
  else
  {
    result = MPI_Gather(v, n, MPI_DOUBLE, rv, rn, MPI_DOUBLE, root, comm);
  }

  return result;
}


int mlton_MPI_SendGatherv_char (char *sendbuf, size_t sn, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_CHAR, NULL, NULL, NULL, MPI_DATATYPE_NULL, root, comm);

  return result;
}

int mlton_MPI_RecvGatherv_char (char *sendbuf, size_t sn, char *recvbuf, 
                                int *recvcounts, int *displs, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_CHAR, recvbuf, recvcounts, displs, MPI_CHAR, root, comm);

  return result;
}

int mlton_MPI_SendGatherv_int (int *sendbuf, size_t sn, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_INT, NULL, NULL, NULL, MPI_DATATYPE_NULL, root, comm);

  return result;
}

int mlton_MPI_RecvGatherv_int (int *sendbuf, size_t sn, int *recvbuf, 
                                int *recvcounts, int *displs, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_INT, recvbuf, recvcounts, displs, MPI_INT, root, comm);

  return result;
}

int mlton_MPI_SendGatherv_long (long int *sendbuf, size_t sn, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_LONG, NULL, NULL, NULL, MPI_DATATYPE_NULL, root, comm);

  return result;
}

int mlton_MPI_RecvGatherv_long (long int *sendbuf, size_t sn, long int *recvbuf, 
                                int *recvcounts, int *displs, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_LONG, recvbuf, recvcounts, displs, MPI_LONG, root, comm);

  return result;
}


int mlton_MPI_SendGatherv_double (double *sendbuf, size_t sn, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_DOUBLE, NULL, NULL, NULL, MPI_DATATYPE_NULL, root, comm);

  return result;
}

int mlton_MPI_RecvGatherv_double (double *sendbuf, size_t sn, double *recvbuf, 
                                  int *recvcounts, int *displs, int root, MPI_Comm comm)
{
  int result; 

  result = MPI_Gatherv(sendbuf, sn, MPI_DOUBLE, recvbuf, recvcounts, displs, MPI_DOUBLE, root, comm);

  return result;
}

