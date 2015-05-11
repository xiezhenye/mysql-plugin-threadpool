# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

#threadpool_common.cc 
#IF(WIN32) 
#  SET(SQL_SHARED_SOURCES ${SQL_SHARED_SOURCES} threadpool_win.cc) 
#ELSE() 
# SET(SQL_SHARED_SOURCES ${SQL_SHARED_SOURCES} threadpool_unix.cc) 
#ENDIF()

MYSQL_ADD_PLUGIN(threadpool threadpool_common.cc threadpool_unix.cc
  MODULE_ONLY MODULE_OUTPUT_NAME "threadpool")

