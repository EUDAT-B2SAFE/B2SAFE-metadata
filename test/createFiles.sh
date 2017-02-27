#!/bin/bash

#workspace
WORK_DIR=$1
#number of files
MAX_FILES=100
#number of dirs
MAX_DIR=10

for k in $(seq 1 $MAX_DIR); do
    dir_path=$WORK_DIR/dir$( printf %03d "$k" );
    mkdir $dir_path;
    for n in $(seq 1 $MAX_FILES); do
        dd if=/dev/urandom of=$dir_path/file$( printf %03d "$n" ).bin bs=1k count=1;
    done
done

#metadata
metadir_path=$WORK_DIR/metadir;
mkdir $metadir_path
for i in $(seq 1 $MAX_DIR); do
    dd if=/dev/urandom of=$metadir_path/file$( printf %03d "$i" ).meta bs=1k count=1;
done
