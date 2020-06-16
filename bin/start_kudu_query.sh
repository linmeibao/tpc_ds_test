#!/bin/bash

basedir="/root/tpc_ds_test/"

# 分隔查询的sql文件
file_name="${basedir}query_data/query_0.sql"

# 开头字符串1
start_str1="SELECT"
# 开头字符串2
start_str2="WITH"
# 结尾字符串
end_str=";"

# 获取到SQL语句开头的标识
start_flag=""
condition=""

# SQL查询语句目录
query_dir="${basedir}sql/query/"
query_file=""
query_prefix="${query_dir}query_s_"
query_suffix=".sql"

# SQL查询语句日志目录
query_log_dir="${basedir}logs/query/"

# 结果文件
query_result_file="${basedir}result/query_result.csv"

i=0

rm $query_result_file

# 是否清理SQL查询语句目录
clear=true

if [[ "${clear}" == true ]];
then
    echo "clear..."
    rm -rf "${query_dir}"*
fi

while read -r line || [[ -n ${line} ]]
do

    if [[ "${start_flag}" == "" ]];
    then
        condition=$(echo $line | grep -i "${start_str1}\|${start_str2}")
        if [[ "$condition" != "" ]] && [[ $condition != *"--"* ]];
        then
#          echo "START-- $line"
          start_flag="1"
          query_file=$query_prefix$i$query_suffix
          echo "query_file $query_file"
	  echo "use tpc_test;" >> $query_file
          echo "${line}" >> $query_file
          i=$[$i+1]
        fi
    else
        condition=$(echo $line | grep -i "${end_str}")
        if [[ "$condition" != "" ]] && [[ $condition != *"--"* ]];
        then
#          echo "END-- $line"
          echo "${line}" >> $query_file
          start_flag=""
        else
#          echo "CONTENT $line"
          echo "${line}" >> $query_file
        fi
    fi

done < $file_name

echo "文件名,SQL,执行时长" >> $query_result_file


for file in $(ls $query_dir)
do
    echo "fileName：$file"

    sql=$(cat $query_dir$file | xargs echo -n)
    sql=${sql//,/，}
#    echo "SQL===============${sql}"
    log_file=${file%.*}.log
    impala-shell -f $query_dir$file > $query_log_dir$log_file 2>&1
#    echo "$query_dir$file > $query_log_dir${file%.*}.log 2>&1"
    exec_info=$(cat $query_log_dir${file%.*}.log | grep Fetched)
    echo "$exec_info"
#    exec_info="Fetched"
    echo "$query_dir$file,$sql,$exec_info" >> $query_result_file
done

