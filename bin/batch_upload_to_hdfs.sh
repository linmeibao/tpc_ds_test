#!/bin/bash

#文件存放的目录
data_src_dir=/root/test_datas/

#文件上传到hdfs的根路径
hdfs_root_dir=/tmp/test/

file_dir=""

hdfs dfs -mkdir $hdfs_root_dir

ls $data_src_dir | while read fileName
do
    file_dir=${fileName%.*}
    echo "${file_dir}..."
    hdfs dfs -mkdir $hdfs_root_dir$file_dir
    echo "${fileName}..."
    hdfs dfs -put $data_src_dir$fileName $hdfs_root_dir$file_dir/$fileName
done

echo "The upload to HDFS is complete..."
