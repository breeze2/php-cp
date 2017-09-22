dnl $Id$
dnl config.m4 for extension connect_pool

dnl Comments in this file start with the string 'dnl'.
dnl Remove where necessary. This file will not work
dnl without editing.

dnl If your extension references something external, use with:

PHP_ARG_WITH(connect_pool, for connect_pool support,
[  --with-connect_pool             Include connect_pool support])

dnl Otherwise use enable:

dnl PHP_ARG_ENABLE(connect_pool, whether to enable connect_pool support,
dnl Make sure that the comment is aligned:
dnl [  --enable-connect_pool           Enable connect_pool support])

if test "$PHP_CONNECT_POOL" != "no"; then

AC_MSG_CHECKING(PHP version)
AC_TRY_COMPILE([#include "$phpincludedir/main/php_version.h"], [
#if PHP_MAJOR_VERSION < 5
#error  this extension requires at least PHP version 5 or newer
#endif
],
[AC_MSG_RESULT(ok)],
[AC_MSG_ERROR([need at least PHP 5 or newer])])


AC_MSG_CHECKING(ZTS)
AC_TRY_COMPILE([#include "$phpincludedir/main/php_config.h"], [
#ifdef ZTS
#error  this extension requires no zts, please do not add ' --enable-maintainer-zts' when you configure php
#endif
],
[AC_MSG_RESULT(ok)],
[AC_MSG_ERROR([need php no zts, please do not add ' --enable-maintainer-zts' when you configure php])])

    CFLAGS="-Wall -pthread $CFLAGS"
    LDFLAGS="$LDFLAGS -lpthread"

  PHP_NEW_EXTENSION(connect_pool, connect_pool.c cpServer.c cpWorker.c \
                    connect_pool_client.c \
                    cpFunction.c \
                    cpMemory.c \
                    cpNetWork.c \
                    cpClientNet.c \
                    cpPingWorker.c \
                    msgpack/msgpack.c \
                    msgpack/msgpack_pack.c\
                    msgpack/msgpack_unpack.c\
                    msgpack/msgpack_convert.c\
                    , $ext_shared)
  PHP_ADD_INCLUDE([$ext_srcdir/include])
fi
