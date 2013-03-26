Colog is a log file aggregator.  It reads and merges log files from
different servers and outputs them to stdout in a mostly-sorted
fashion.

Usage
=====

Your S3 access credentials must be provided using the environment
variables:

  - `AWS_ACCESS_KEY`: account access key
  - `AWS_SECRET_KEY`: secret

The basic arguments are:

    colog s3://<bucketname>/<logroot> <start-date>[/[<end-date>]]

Examples:

    # List all the logs from all servers produced between March 1st
    # and March 2nd in 2013
    colog s3://mybucket/logs/ 2013-03-01/2013-03-02

    # List all the logs for 3 March 2013, 14:45 UTC
    colog s3://mybucket/logs/ 2013-03-03T14:45

    # List all the logs from march until now
    colog s3://mybucket/logs/ 2013-03/...

For more info type:

    colog --help

Filtering by seconds is currently not supported.


Structure of log file store
===========================

Colog currently assumes that all log files are stored on Amazon S3 and
requires the following directory structure:

    s3://<bucket>/<root>/<servername>/<yyyy-mm-dd>/<hh>/<mm>Z

for example the file:

    s3://mybucket/syslogs/i-39824792/2013-03-15/14/43Z

would contain the syslogs for server `i-39824792` on 15 March 2013,
14:43 UTC time.  The log file itself must be a bzip2-compressed CSV
file.  Each line in the CSV file must start with the date in ISO 8601
format using UTC time.  For example:

    2013-02-17T15:58:00.049808+00:00,01.myserver.mydomain,user,notice,"label",12345,"My message here"

Colog only requires that the first component in each line is in ISO
8601 format for sorting purposes.  The remainder of the line is
currently ignored.


Installation
============

    $ hsenv --ghc=7.6.2
    $ source .hsenv/bin/activate
    $ cabal update
    $ make install INSTALL_OPTS=-j

