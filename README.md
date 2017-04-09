cafs
====
[![Build Status](https://travis-ci.org/indyjo/cafs.svg)](https://travis-ci.org/indyjo/cafs)

Content-Addressable File System.

This is the data caching back-end used by the BitWrk distributed computing
software. See https://bitwrk.net/ for more info.

Stores data in de-duplicated form and provides a remote-synching mechanism with
another CAFS instance.

Data no longer referenced is kept in cache until the space is needed.
Currently, data is not saved to persistent storage.