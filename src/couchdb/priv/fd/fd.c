/*

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "erl_nif.h"

ERL_NIF_TERM make_error(ErlNifEnv* env, int err)
{
  ERL_NIF_TERM term;

  switch (err)
  {
  case ENOENT:
    term = enif_make_atom(env, "enoent");
    break;
  default:
    term = enif_make_string(env, strerror(err), ERL_NIF_LATIN1);
  }

  return enif_make_tuple(env, 2, enif_make_atom(env, "error"), term);
}

static ERL_NIF_TERM open_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ERL_NIF_TERM tail, opt;
  char fname[MAXPATHLEN];
  char str[64];
  int flags = O_RDONLY | O_LARGEFILE;
  int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH;
  int fd;

  if (enif_get_string(env, argv[0],  fname, MAXPATHLEN, ERL_NIF_LATIN1) <= 0)
    return enif_make_badarg(env);

  if (!enif_is_list(env, argv[1]))
    return enif_make_badarg(env);

  /* parse options */

  tail = argv[1];

  while (enif_get_list_cell(env, tail, &opt, &tail))
  {
    if (enif_get_atom(env, opt, str, sizeof(str), ERL_NIF_LATIN1) <= 0)
      return enif_make_badarg(env);

    if (!strcmp(str, "write"))
      flags |= O_RDWR|O_CREAT;
    if (!strcmp(str, "create"))
      flags |= O_RDWR|O_CREAT;
    else if (!strcmp(str, "append"))
      flags |= O_RDWR|O_APPEND;
  }

  if ((fd = open(fname, flags, mode)) >= 0)
    return enif_make_tuple(env, 2, 
                           enif_make_atom(env, "ok"),
                           enif_make_int(env, fd));
  else
    return make_error(env, errno);
}

static ERL_NIF_TERM close_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int fd;

  if (!enif_get_int(env, argv[0], &fd))
    return enif_make_badarg(env);

  close(fd);

  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM sync_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int fd;

  if (!enif_get_int(env, argv[0], &fd))
    return enif_make_badarg(env);

  #ifdef __APPLE__
  fcntl(fd, F_FULLFSYNC);
  #else
  if (0 != fsync(fd)) {
    return enif_make_tuple(
      env,
      2,
      enif_make_atom(env, "fsync_error"),
      /* enif_make_atom(env, sys_errlist[errno]) */
      enif_make_int(env, errno)
    );
  }
  #endif

  return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM truncate_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int fd;
  ErlNifSInt64 pos;

  if (!enif_get_int(env, argv[0], &fd))
    return enif_make_badarg(env);

  if (argc > 1 && !enif_get_int64(env, argv[1], &pos))
    return enif_make_badarg(env);

  if (0 == ftruncate(fd, pos)) {
    return enif_make_atom(env, "ok");
  }

  return enif_make_tuple(
    env,
    2,
    enif_make_atom(env, "ftruncate_error"),
    /* enif_make_atom(env, sys_errlist[errno]) */
    enif_make_int(env, errno)
  );
}

static ERL_NIF_TERM position_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int fd;
  ErlNifSInt64 pos;
  char eof[4];
  
  if (!enif_get_int(env, argv[0], &fd))
    return enif_make_badarg(env);

  /* fd:position(Fd) */
  
  if (argc == 1) 
    return enif_make_tuple(env, 2, 
                           enif_make_atom(env, "ok"),
                           enif_make_int64(env, lseek(fd, 0, SEEK_CUR)));

  /* fd:position(Fd, eof) */
  
  if (enif_is_atom(env, argv[1]))
  {
    if (enif_get_atom(env, argv[1], eof, 4, ERL_NIF_LATIN1) <= 0
        || strcmp(eof, "eof"))
      return enif_make_badarg(env);
    else
      return enif_make_tuple(env, 2, 
                             enif_make_atom(env, "ok"),
                             enif_make_int64(env, lseek(fd, 0, SEEK_END)));
  }
  
  /* fd:position(Fd, Pos) */
  
  if (!enif_get_int64(env, argv[1], &pos))
    return enif_make_badarg(env);
  
  return enif_make_tuple(env, 2, 
                         enif_make_atom(env, "ok"),
                         enif_make_int(env, lseek(fd, pos, SEEK_SET)));
}

static ERL_NIF_TERM write_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int fd;
  ErlNifBinary bin;

  if (!enif_get_int(env, argv[0], &fd))
    return enif_make_badarg(env);

  if (!enif_inspect_iolist_as_binary(env, argv[1], &bin))
    return enif_make_badarg(env);

  if (write(fd, bin.data, bin.size) >= 0)
    return enif_make_atom(env, "ok");
  else
    return make_error(env, errno);
}

static ERL_NIF_TERM pwrite_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int fd;
  ErlNifSInt64 pos;
  ErlNifBinary bin;
  ssize_t written;

  if (!enif_get_int(env, argv[0], &fd))
    return enif_make_badarg(env);

  if (!enif_get_int64(env, argv[1], &pos))
    return enif_make_badarg(env);

  if (!enif_inspect_iolist_as_binary(env, argv[2], &bin))
    return enif_make_badarg(env);

  written = pwrite(fd, bin.data, bin.size, pos);

  if (written < 0) {
      return enif_make_tuple(env, 2,
                             enif_make_atom(env, "pwrite_error"),
                             enif_make_int(env, errno));
  }

  return enif_make_tuple(env, 2, 
                         enif_make_atom(env, "ok"),
                         enif_make_long(env, written));
}

int do_pread(ErlNifEnv* env, int fd, 
             ERL_NIF_TERM pos_term, ERL_NIF_TERM size_term,
             ERL_NIF_TERM* data)
{
  ErlNifSInt64 pos;
  size_t size;
  ErlNifBinary bin;  
  size_t result;

  if (!enif_get_int64(env, pos_term, &pos))
    return enif_make_badarg(env);
    
  if (!enif_get_long(env, size_term, &size))
    return enif_make_badarg(env);
    
  if (!enif_alloc_binary(size, &bin))
    return enif_make_tuple(env, 2, 
                           enif_make_atom(env, "error"),
                           enif_make_atom(env, "alloc"));
    
  if ((result = pread(fd, bin.data, bin.size, pos)) < 0)
  {
    enif_release_binary(&bin);
    *data = make_error(env, result);

    return 0;
  }
  else
  {
    *data = enif_make_binary(env, &bin);
    return 1;
  }
}

static ERL_NIF_TERM pread2_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int arity, fd;
  long pos;
  size_t size;
  ErlNifBinary bin;  
  ERL_NIF_TERM result, tail, head, data;
  const ERL_NIF_TERM *tuple;

  if (!enif_get_int(env, argv[0], &fd))
    return enif_make_badarg(env);

  if (!enif_is_list(env, argv[1]))
    return enif_make_badarg(env);
    
  tail = argv[1];
  result = enif_make_list(env, 0);
  
  while (enif_get_list_cell(env, tail, &head, &tail))
  {
    if (!enif_get_tuple(env, head, &arity, &tuple)
        || arity != 2)
      return enif_make_badarg(env);
    
    if (!do_pread(env, fd, tuple[0], tuple[1], &data))
      /* XXX should free the result list and binaries here! */
      return data;

    result = enif_make_list_cell(env, data, result);
  }
  
  return enif_make_tuple(env, 2, enif_make_atom(env, "ok"), result);
}

static ERL_NIF_TERM pread3_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  int fd;
  long pos;
  size_t size, result;
  ERL_NIF_TERM data;
  
  if (!enif_get_int(env, argv[0], &fd))
    return enif_make_badarg(env);

  if (!do_pread(env, fd, argv[1], argv[2], &data))
    return data;
  else
    return enif_make_tuple(env, 2, enif_make_atom(env, "ok"), data);
}

static ErlNifFunc nifs[] = {
  {"open", 2, open_nif},
  {"close", 1, close_nif},
  {"sync", 1, sync_nif},
  {"truncate", 2, truncate_nif},
  {"position", 1, position_nif},
  {"position", 2, position_nif},
  {"write", 2, write_nif},
  {"pwrite", 3, pwrite_nif},
  {"pread3", 3, pread3_nif},
  {"pread2", 2, pread2_nif}
};

ERL_NIF_INIT(fd, nifs, NULL, NULL, NULL, NULL)

