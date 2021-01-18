# gobitcast

[bitcast intro paper](https://riak.com/assets/bitcask-intro.pdf)

active datafile set policy:
* if db dir is empty, create 0.dat and set it as the max fileid
* if db dir not empty(exp: with 0.dat,1.data) ,set the max fileid as the active fd 
    if filesize of  max  fileid exceed the maxSize of datafile , max fileid + 1 and update active datafile 
