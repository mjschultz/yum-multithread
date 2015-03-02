#!/usr/bin/python
#
# Version: 0.4
#
# A plugin for the Yellowdog Updater Modified that creates multiple download
# threads to acquire packages needing updates.
#
# To install this plugin, just drop it into /usr/lib/yum-plugins, and
# make sure you have 'plugins=1' in your /etc/yum.conf.  You also need to
# create the following configuration file, if not installed through an RPM:
#
#  /etc/yum/pluginconf.d/multithread.conf:
#   [main]
#   enabled=1
#   verbose=0
#   dl_timeout=300
#   socket_timeout=10
#   max_threads=8
#   threads_per_server=2
#   servers_per_repo=4
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# (C) Copyright 2010 Michael J. Schultz <mjschultz@gmail.com>
#

"""
B{multithread} is a Yum plugin that uses multiple threads to download RPM
packages.
"""

import os
import sys
import time
import socket
import string
import urlparse
import datetime
import threading
import pycurl

from yum.plugins import TYPE_CORE

requires_api_version = '2.5'
plugin_type = (TYPE_CORE,)

verbose = False
dl_timeout = 10
socket_timeout = 10
max_threads = 8
threads_per_server = 2
servers_per_repo = 4

def init_hook(conduit):
    """
    This function initiliazes the variables required for running B{multithread}
    module. The variables are initiliazed from the main section of the plugin file.

    There are no parameteres for this function. It uses global variables to
    communicate with other functions.

    This function refers:
        - L{get_hostfile_age}

    @param verbose : Verbosity of output.
    @type verbose : Boolean
    @param socket_timeout : The default timeout for a socket connection.
    @type socket_timeout : Integer
    @param dl_timeout : The default timeout for a download to complete.
    @type dl_timeout : Integer
    @param max_threads : The number of threads to handle downloading.
    @type max_threads : Integer
    @param threads_per_server : Most threads connecting to one server
    @type threads_per_server : Integer
    @param servers_per_repo : Most servers to select for a single repository
    @type servers_per_repo : Integer

    """
    global verbose, socket_timeout, dl_timeout, max_threads
    global servers_per_repo, threads_per_server
    if hasattr(conduit, 'registerPackageName'):
        conduit.registerPackageName("yum-plugin-multithread")
    verbose = conduit.confBool('main', 'verbose', default=False)
    socket_timeout = conduit.confInt('main', 'socket_timeout', default=10)
    dl_timeout = conduit.confInt('main', 'dl_timeout', default=300)
    max_threads = conduit.confInt('main', 'max_threads', default=8)
    threads_per_server = conduit.confInt('main', 'threads_per_server',
                                         default=4)
    servers_per_repo = conduit.confInt('main', 'servers_per_repo', default=4)

def predownload_hook(conduit):
    """
    This function is called before Yum begins downloading required packages.

    There are no parameters for this function. It uses global variables to
    communicate with other functions.

    This function referes:
        - L{...}
    """
    global servers_per_repo

    # Fetch a listing of repositories
    repolist = conduit.getRepos()

    # Fetch a listing of packages to download
    pkglist = conduit.getDownloadPackages()

    mt = MultiThread()
    url_pos = dict()

    # Setup a queue of packages to download
    for pkg in pkglist :
        # No need to download if pkg already on disk
        if (True == pkg.verifyLocalPkg()) :
            continue
        rid = pkg.repoid
        if rid not in url_pos :
            url_pos[rid] = 0
        url = repolist.getRepo(rid).urls[url_pos[rid]]

        # It is possible that a repo has fewer than servers_per_repo
        nrepos = min(servers_per_repo, len(repolist.getRepo(rid).urls))
        url_pos[rid] = (url_pos[rid] + 1) % nrepos

        local = pkg.localPkg()
        remote = url + '/' + pkg.relativepath
        mt.add_package(remote, local)

    start = time.time() # The clock is started.
    mt.fetch_packages()
    stop = time.time() # The clock is stopped.
    print 'Elapsed time is: %.1f seconds.' % (stop - start)

class MultiThread:
    """
    This is the helper class of B{multithread} module. This class handles
    the spawning and processing of all downloads.
    """

    def __init__(self):
        """
        This is the initiliazer function of the B{L{MultiThread}} class.
        """
        global max_threads, socket_timeout, dl_timeout

        self.mc = pycurl.CurlMulti()
        self.url_queue = dict()
        self.npackages = 0

        # Prepare a ring of download handlers for packages
        self.mc.handles = []
        for i in range(max_threads) :
            sc = pycurl.Curl()
            sc.fp = None
            sc.setopt(pycurl.FOLLOWLOCATION, 1)
            sc.setopt(pycurl.MAXREDIRS, 5)
            sc.setopt(pycurl.CONNECTTIMEOUT, socket_timeout)
            sc.setopt(pycurl.TIMEOUT, dl_timeout)
            sc.setopt(pycurl.NOSIGNAL, 1)
            self.mc.handles.append(sc)

    def add_package(self, remote, local) :
        """
        This function adds a package to the download queue.  It parses the
        remote url, and adds the download to a queue for that server.

        @param remote : Full URI path of file to download
        @type remote : String
        @param local : Local path to save downloaded file to
        @type local : String
        """
        # parse the url, get the server
        url = urlparse.urlparse(remote)
        server = url.netloc
        # check if server exists in the url_queue[server]
        if server not in self.url_queue :
            self.url_queue[server] = []
        # url_queue[server] exists now!
        self.url_queue[server].append((remote, local))
        self.npackages += 1

    def fetch_packages(self) :
        """
        This function will begin the download process by spawning up to
        max_connections.  As downloads complete, it reclaims a single curl
        thread and reallocates it to begin a new download.  This will block
        until all downloads have completed (or have all timed out).
        """
        global verbose
        curllist = self.mc.handles[:]
        serverlist = []
        # build a list of "free" servers (those that can be downloaded from)
        for server in self.url_queue :
            thr_max = min(threads_per_server, len(self.url_queue[server]))
            for i in range(thr_max) :
                serverlist.append(server)
        # Now, handle the downloads
        processed = 0
        active = 0
        success = 0
        failed = 0
        while processed < self.npackages :
            # Process as many pkgs as free list allows
            while curllist and serverlist :
                server = serverlist.pop(0)
                if not self.url_queue[server] :
                    continue
                remote, local = self.url_queue[server].pop(0)
                sc = curllist.pop(0)
                sc.fp = open(local, 'wb')
                sc.setopt(pycurl.URL, remote)
                sc.setopt(pycurl.WRITEDATA, sc.fp)
                self.mc.add_handle(sc)
                active += 1
                if verbose :
                    ess = "s" if active != 1 else " "
                    print '['+str(active),'active download'+ess+']',
                    print 'Fetching:', os.path.basename(local),'from',server
                sc.local = local
                sc.remote = remote
                sc.server = server
            # Let curl do its thing
            while 1 :
                ret, num_handles = self.mc.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM :
                    break
            # Keep watching curl objects for termination, adding to freelist
            # when done
            while 1 :
                num_q, ok_list, err_list = self.mc.info_read()
                for sc in ok_list :
                    sc.fp.close()
                    sc.fp = None
                    self.mc.remove_handle(sc)
                    if verbose :
                        print '['+str(success+1)+'/'+str(self.npackages)+']',
                        print 'Completed:', os.path.basename(sc.local)
                    if len(self.url_queue[sc.server]) > 0 :
                        serverlist.append(sc.server)
                    curllist.append(sc)
                for sc, errno, errmsg in err_list :
                    sc.fp.close()
                    sc.fp = None
                    self.mc.remove_handle(sc)
                    if verbose :
                        print '['+str(success+1)+'/'+str(self.npackages)+']',
                        print 'Failed:',os.path.basename(sc.local),
                        print '('+errmsg+')'
                    if len(self.url_queue[sc.server]) > 0 :
                        serverlist.append(sc.server)
                    curllist.append(sc)
                success = success + len(ok_list)
                failed = failed + len(err_list)
                active = active - len(ok_list) - len(err_list)
                processed = failed + success
                if num_q == 0 :
                    break
            # Could show progress bar here.
            if not verbose :
                ess = "s" if active != 1 else " "
                print '\rDownloaded:',success,'of',self.npackages,'packages',
                print 'with', active, 'active download'+ess,
            # Sleep until some more data is available
            self.mc.select(1.0)
        if not verbose :
            print '' # newline

        # Download process is complete, preform some cleanup 
        for sc in self.mc.handles :
            if sc.fp != None :
                sc.fp.close()
                sc.fp = None
            sc.close()
        self.mc.close()

def main():
    """
    This is the main function for B{multithread} module.

    This function explains the usage of the B{multithread} module. It also
    parses command line arguments.

    This function refers:
        - L{MultiThread.add_package()}
        - L{MultiThread.fetch_packages()}
    """
    global verbose
    verbose = True

    if len(sys.argv) == 1:
        print 'Usage: %s <download-file>' % sys.argv[0]
        print 'This will download all files in <download-file>',
        print 'to ./mt_downloads/<basename>'
        sys.exit(1)

    local_dir = './mt_downloads'
    if not os.path.exists(local_dir) :
        os.mkdir(local_dir)

    mt = MultiThread()
    f = open(sys.argv[1], 'r')
    file_list = f.readlines()
    f.close()

    for file in file_list :
        remote = file.strip()
        url = urlparse.urlparse(remote)
        basename = os.path.basename(url.path)
        local = local_dir+'/'+basename
        mt.add_package(remote, local)

    mt.fetch_packages()

if __name__ == '__main__':
    main()
