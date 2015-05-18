# mysql-plugin-threadpool

## Why thread pool
By default, MySQL using one thread to handle each connection to execute queries. As more client connections, more threads are created. Performance will degrades because of context switching, memory usage, resouce contention. A thread pool will reduces the creation of theads and improves performance on high concurrency scane. MySQL Enterprise provides a thread pool plugin, and some thirdpart fork like percona server, mariadb also provides this feature. But if you are using MySQL Community, this plugin will helps. 

## Usage

CAUTION: 

mysql version >= 5.6.20 is REQUIRED

mysql plugins MUST be built using the same version of the source code and the same build arguments. If mysqld is built as a debug version without cmake parameter -DBUILD_CONFIG, the parameter must not be added when compiling plugins.

First, compile the plugin and install in to plugin dir

    cp -r src /path/to/mysql-src/plugin/threadpool
    cd /path/to/mysql-src
    cmake . -DBUILD_CONFIG=mysql_release
    cd plugin/threadpool
    make
    make install
    
Then, load the plugin into mysql

    mysql> INSTALL PLUGIN THREADPOOL SONAME 'threadpool.so';
    
