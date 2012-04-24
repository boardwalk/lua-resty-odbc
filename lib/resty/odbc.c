#define _POSIX_SOURCE // TODO this probably needs cleaning up
#define _POSIX_C_SOURCE 200112L

#include <lua.h>
#include <lauxlib.h>
#include <sql.h>
#include <sqlext.h>

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// pushes the message text of a sql error onto the lua stack
static void lua_sql_error(lua_State* L, SQLSMALLINT handle_type, SQLHANDLE handle)
{
  SQLSMALLINT recnum;
  for(recnum = 1; /**/; recnum++)
  {
    char buf[1024];
    SQLRETURN ret = SQLGetDiagField(handle_type, handle, recnum,
        SQL_DIAG_MESSAGE_TEXT, buf, sizeof(buf), NULL);

    if(!SQL_SUCCEEDED(ret))
      break;

    if(recnum != 1)
      lua_pushstring(L, "; ");

    lua_pushstring(L, buf);
  }

  if(recnum == 1)
    lua_pushstring(L, "unknown sql error");
  else
    lua_concat(L, 2 * recnum - 3);
}

// push a value from a result set onto the lua stack
static void lua_sql_pushresult(lua_State* L, SQLHSTMT hstmt, SQLSMALLINT col)
{
  // we have old header here that defines SQLLEN as a 4 byte integer,
  // which is wrong and smashes the stack here
  /*SQLLEN*/long col_type;
  SQLColAttribute(hstmt, col, SQL_DESC_CONCISE_TYPE, NULL, 0, NULL, &col_type);

  col_type = labs(col_type); // TODO Why do we need this?

  if(col_type == SQL_CHAR ||
      col_type == SQL_VARCHAR){
    SQLCHAR val[256];
    SQLGetData(hstmt, col, SQL_C_CHAR, val, sizeof(val), NULL);
    lua_pushstring(L, (char*)val);
  }
  if(col_type == SQL_INTEGER) {
    SQLINTEGER val;
    SQLGetData(hstmt, col, SQL_C_SLONG, &val, 0, NULL);
    lua_pushinteger(L, val);
  }
  else if(col_type == SQL_SMALLINT) {
    SQLSMALLINT val;
    SQLGetData(hstmt, col, SQL_C_SSHORT, &val, 0, NULL);
    lua_pushinteger(L, val);
  }
  else if(col_type == SQL_FLOAT ||
      col_type == SQL_REAL ||
      col_type == SQL_DOUBLE) {
    SQLDOUBLE val;
    SQLGetData(hstmt, col, SQL_C_DOUBLE, &val, 0, NULL);
    lua_pushnumber(L, val);
  }
  else if(col_type == SQL_TYPE_DATE) {
    DATE_STRUCT val;
    SQLGetData(hstmt, col, SQL_C_TYPE_DATE, &val, 0, NULL);
    char date[16];
    sprintf(date, "%04d-%02d-%02d", val.year, val.month, val.day);
    lua_pushstring(L, date);
  }
  else if(col_type == SQL_TYPE_TIME) {
    TIME_STRUCT val;
    SQLGetData(hstmt, col, SQL_C_TYPE_TIME, &val, 0, NULL);
    char time[16];
    sprintf(time, "%02d:%02d:%02d", val.hour, val.minute, val.second);
    lua_pushstring(L, time);
  }
  else if(col_type == SQL_TYPE_TIMESTAMP) {
    TIMESTAMP_STRUCT val;
    SQLGetData(hstmt, col, SQL_C_TIMESTAMP, &val, 0, NULL);
    char timestamp[32];
    sprintf(timestamp, "%04d-%02d-%02d %02d:%02d:%02d", val.year, val.month, val.day,
        val.hour, val.minute, val.second);
    lua_pushstring(L, timestamp);
  }
  else {
    fprintf(stderr, "unknown data type %ld\n", col_type);
    lua_pushnil(L);
  }
}

// returns a array of file descriptors for open sockets terminated by a -1 sentinel
static int* get_open_sockets()
{
  char dir_path[64];
  sprintf(dir_path, "/proc/%d/fd", getpid());

  size_t fd_list_size = 0;
  size_t fd_list_capacity = 8;
  int* fd_list = (int*)malloc(fd_list_capacity * sizeof(int));

  DIR* d = opendir(dir_path);
  if(d)
  {
    for(;;) {
      struct dirent de, *pde;
      readdir_r(d, &de, &pde);
      if(!pde)
        break;

      char fd_path[64];
      sprintf(fd_path, "%s/%s", dir_path, de.d_name);

      char link[64];
      ssize_t link_size = readlink(fd_path, link, sizeof(link) - 1);
      if(link_size < 0)
        continue;
      link[link_size] = '\0';

      if(strstr(link, "socket:") != link)
        continue;

      if(fd_list_size == fd_list_capacity) {
        fd_list_capacity *= 2;
        fd_list = (int*)realloc(fd_list, fd_list_capacity * sizeof(int));
      }

      fd_list[fd_list_size++] = atoi(de.d_name);
    }

    closedir(d);
  }

  fd_list[fd_list_size] = -1;
  return fd_list;
}

// v in a?
static int array_contains(const int* a, int v)
{
  for(int i = 0; a[i] != -1; i++)
  {
    if(a[i] == v)
      return 1;
  }
  return 0;
}

// a = a \ b
static void array_set_subtract(int* a, const int* b)
{
  int j = 0;
  for(int i = 0; a[i] != -1; i++)
  {
    if(!array_contains(b, a[i])) {
      a[j++] = a[i];
    }
  }
  a[j] = -1;
}

/*
 * cursor
 */
typedef struct
{
  SQLHSTMT hstmt;
} cursor_t;

static int cursor_gc(lua_State* L)
{
  cursor_t* c = (cursor_t*)lua_topointer(L, 1);

  SQLFreeHandle(SQL_HANDLE_STMT, c->hstmt);

  lua_pushlightuserdata(L, c);
  lua_pushnil(L);
  lua_settable(L, LUA_REGISTRYINDEX);

  return 0;
}

// row = cursor:fetch([table])
static int cursor_fetch(lua_State* L)
{
  cursor_t* c = (cursor_t*)luaL_checkudata(L, 1, "resty.odbc.cursor");

  SQLRETURN ret = SQLFetch(c->hstmt);

  if(ret == SQL_NO_DATA) {
    lua_pushnil(L);
    return 1;
  }
  
  // TODO Handle SQL_STILL_EXECUTING

  if(!SQL_SUCCEEDED(ret)) {
    lua_sql_error(L, SQL_HANDLE_STMT, c->hstmt);
    return lua_error(L);
  }

  SQLSMALLINT col_count;
  SQLNumResultCols(c->hstmt, &col_count);

  if(lua_istable(L, 2))
    lua_pushvalue(L, 2);
  else
    lua_createtable(L, col_count, 0);

  for(SQLSMALLINT i = 1; i <= col_count; i++) {
    lua_pushinteger(L, i);
    lua_sql_pushresult(L, c->hstmt, i);
    lua_settable(L, -3);
  }

  return 1;
}

/*
 * conn
 */
typedef struct
{
  SQLHDBC hdbc;
  int fd;
  int async_stmt;
} conn_t;

static int conn_gc(lua_State* L)
{
  conn_t* c = (conn_t*)lua_topointer(L, 1);

  SQLFreeHandle(SQL_HANDLE_DBC, c->hdbc);

  lua_pushlightuserdata(L, c);
  lua_pushnil(L);
  lua_settable(L, LUA_REGISTRYINDEX);

  return 0;
}

// cursor = conn:execute(query)
static int conn_execute(lua_State* L)
{
  conn_t* c = (conn_t*)luaL_checkudata(L, 1, "resty.odbc.conn");
  const char* query = luaL_checkstring(L, 2);

  SQLHSTMT hstmt;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, c->hdbc, &hstmt);

  if(!SQL_SUCCEEDED(ret)) {
    lua_sql_error(L, SQL_HANDLE_DBC, c->hdbc);
    return lua_error(L);
  }

  if(c->async_stmt) {
    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_ASYNC_ENABLE,
        (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0);

    if(!SQL_SUCCEEDED(ret)) {
      lua_sql_error(L, SQL_HANDLE_STMT, hstmt);
      SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      return lua_error(L);
    }
  }

  ret = SQLExecDirect(hstmt, (SQLCHAR*)query, SQL_NTS);

  if(!SQL_SUCCEEDED(ret)) {
    lua_sql_error(L, SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return lua_error(L);
  }

  cursor_t* cu = (cursor_t*)lua_newuserdata(L, sizeof(cursor_t));
  cu->hstmt = hstmt;

  lua_pushlightuserdata(L, cu);
  lua_pushvalue(L, 1);
  lua_settable(L, LUA_REGISTRYINDEX);

  if(luaL_newmetatable(L, "resty.odbc.cursor")) {
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, cursor_gc);
    lua_setfield(L, -2, "__gc");
    lua_pushcfunction(L, cursor_fetch);
    lua_setfield(L, -2, "fetch");
  }
  lua_setmetatable(L, -2);

  return 1;
}

/*
 * module
 */
typedef struct
{
  SQLHENV henv;
} module_t;

static int module_gc(lua_State* L)
{
  module_t* m = (module_t*)lua_topointer(L, 1);

  SQLFreeHandle(SQL_HANDLE_ENV, m->henv);

  return 0;
}

// conn = module:connect(server, user, pass)
static int module_connect(lua_State* L)
{
  module_t* m = (module_t*)luaL_checkudata(L, 1, "resty.odbc.module");
  const char* server = luaL_checkstring(L, 2);
  const char* user = luaL_checkstring(L, 3);
  const char* pass = luaL_checkstring(L, 4);

  SQLHDBC hdbc;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DBC, m->henv, &hdbc);

  if(!SQL_SUCCEEDED(ret)) {
    lua_sql_error(L, SQL_HANDLE_ENV, m->henv);
    return lua_error(L);
  }

  int* before_sockets = get_open_sockets();

  ret = SQLConnect(hdbc,
      (SQLCHAR*)server, SQL_NTS,
      (SQLCHAR*)user, SQL_NTS,
      (SQLCHAR*)pass, SQL_NTS);

  if(!SQL_SUCCEEDED(ret)) {
    lua_sql_error(L, SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    free(before_sockets);
    return lua_error(L);
  }

  int* after_sockets = get_open_sockets();
  array_set_subtract(after_sockets, before_sockets);

  int fd = (after_sockets[0] >= 0 && after_sockets[1] < 0) ? after_sockets[0] : -1;

  free(before_sockets);
  free(after_sockets);

  if(fd == -1) {
    lua_pushstring(L, "singular new socket not found");
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    return lua_error(L);
  }

  SQLUINTEGER async_mode;
  ret = SQLGetInfo(hdbc, SQL_ASYNC_MODE, &async_mode, 0, NULL);

  if(!SQL_SUCCEEDED(ret)) {
    lua_sql_error(L, SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    return lua_error(L);
  }

  if(async_mode == SQL_AM_CONNECTION)
  {
    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_ASYNC_ENABLE,
        (SQLPOINTER)SQL_ASYNC_ENABLE_ON, 0);

    if(!SQL_SUCCEEDED(ret)) {
      lua_sql_error(L, SQL_HANDLE_DBC, hdbc);
      SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
      return lua_error(L);
    }
  }

  if(async_mode == SQL_AM_NONE)
    fprintf(stderr, "WARNING: driver does not support asynchronous mode\n");

  conn_t* c = (conn_t*)lua_newuserdata(L, sizeof(conn_t));
  c->hdbc = hdbc;
  c->fd = fd;
  c->async_stmt = (async_mode == SQL_AM_STATEMENT) ? 1 : 0;

  lua_pushlightuserdata(L, c);
  lua_pushvalue(L, 1);
  lua_settable(L, LUA_REGISTRYINDEX);

  if(luaL_newmetatable(L, "resty.odbc.conn")) {
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, conn_gc);
    lua_setfield(L, -2, "__gc");
    lua_pushcfunction(L, conn_execute);
    lua_setfield(L, -2, "execute");
  }
  lua_setmetatable(L, -2);

  return 1;
}

int luaopen_resty_odbc(lua_State* L)
{
  SQLHENV henv;
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);

  if(!SQL_SUCCEEDED(ret)) {
    lua_pushstring(L, "could not allocate environment");
    return lua_error(L);
  }

  ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

  if(!SQL_SUCCEEDED(ret)) {
    lua_sql_error(L, SQL_HANDLE_ENV, henv);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
    return lua_error(L);
  }

  module_t* m = (module_t*)lua_newuserdata(L, sizeof(module_t));
  m->henv = henv;

  if(luaL_newmetatable(L, "resty.odbc.module")) {
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    lua_pushcfunction(L, module_gc);
    lua_setfield(L, -2, "__gc");
    lua_pushcfunction(L, module_connect);
    lua_setfield(L, -2, "connect");
  }
  lua_setmetatable(L, -2);

  return 1;
}

