# mysql-plugin-threadpool
MySQL threadpool plugin transplanted from percona server

usage
-----

first, compile the plugin and install in to plugin dir

    cp -r src /path/to/mysql-src/plugin/threadpool
    cd /path/to/mysql-src
    cmake . -DBUILD_CONFIG=mysql_release
    cd plugin/threadpool
    make
    make install
    
CAUTION: mysql plugins MUST be built using the same version of the source code and the same build arguments. If mysqld is built as a debug version without cmake parameter -DBUILD_CONFIG, the parameter must not be added when compiling plugins.
    
then, load the plugin into mysql

    mysql> INSTALL PLUGIN THREADPOOL SONAME 'threadpool.so';
    
