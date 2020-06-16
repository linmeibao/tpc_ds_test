#!/bin/bash

basedir="/root/tpc_ds_test/"

ext_dir="${basedir}sql/ext/"
kudu_dir="${basedir}sql/kudu/"
import_dir="${basedir}sql/import/"

ext_log_dir="${basedir}logs/ext/"
kudu_log_dir="${basedir}logs/kudu/"
import_log_dir="${basedir}logs/import/"

rm -rf "${ext_log_dir}"*
rm -rf "${kudu_log_dir}"*
rm -rf "${import_log_dir}"*

echo "impala-shell -f ${ext_dir}ext_ddl.sql..."
impala-shell -f ${ext_dir}ext_ddl.sql > ${ext_log_dir}ext_ddl.log 2>&1 

echo "impala-shell -f ${ext_dir}ext_count_query.sql -B ..."
impala-shell -f ${ext_dir}ext_count_query.sql -B > ${ext_log_dir}ext_count_query.log 2>&1

echo "impala-shell -f ${ext_dir}ext_limit_query.sql..."
impala-shell -f ${ext_dir}ext_limit_query.sql > ${ext_log_dir}ext_limit_query.log 2>&1

echo "impala-shell -f ${kudu_dir}kudu_ddl.sql..."
impala-shell -f ${kudu_dir}kudu_ddl.sql > ${kudu_log_dir}kudu_ddl.log 2>&1

echo "impala-shell -f ${import_dir}import_test_data.sql..."
impala-shell -f ${import_dir}import_test_data.sql > ${import_log_dir}import_test_data.log 2>&1

echo "impala-shell -f ${kudu_dir}kudu_analyze.sql -B ..."
impala-shell -f ${kudu_dir}kudu_analyze.sql -B > ${kudu_log_dir}kudu_analyze.log 2>&1

echo "impala-shell -f ${kudu_dir}kudu_count_query.sql -B ..."
impala-shell -f ${kudu_dir}kudu_count_query.sql -B > ${kudu_log_dir}kudu_count_query.log 2>&1

echo "impala-shell -f ${kudu_dir}kudu_limit_query.sql..."
impala-shell -f ${kudu_dir}kudu_limit_query.sql > ${kudu_log_dir}kudu_limit_query.log 2>&1

start_str="Query:"
start_flag=""
step="1"
condition=""
content=""

ext_count_result_file="${basedir}result/ext_count_result.csv"

rm -rf $ext_count_result_file

echo "generate ${basedir}result/ext_count_result.csv..."

echo "SQL语句,数据量,执行时长" >> $ext_count_result_file

while read -r line || [[ -n ${line} ]]
do
    #echo "$line"
    if [[ "${start_flag}" == "" ]];
    then
        condition=$(echo $line | grep -i "${start_str}")
	#echo "condition ============== $condition"	
        if [[ "$condition" != "" ]] && [[ "$condition" == *"SELECT"* ]];
        then
            start_flag="1"
            step=$[$step+1]
            content=${line}
        fi
    else
        if [ $step -eq 4 ]
        then
            content=${content},${line}
            step=$[$step+1]
        elif [ $step -eq 5 ]
        then
            content=${content},${line}
            #echo "content===$content"
	        echo "${content}" >> $ext_count_result_file
            start_flag=""
            content=""
            step=1
        else
            step=$[$step+1]
        fi
    fi

done < ${ext_log_dir}ext_count_query.log


kudu_count_result_file="${basedir}result/kudu_count_result.csv"

rm -rf $kudu_count_result_file

echo "generate ${basedir}result/kudu_count_result.csv..."

echo "SQL语句,数据量,执行时长" >> $kudu_count_result_file

while read -r line || [[ -n ${line} ]]
do
    #echo "$line"
    if [[ "${start_flag}" == "" ]];
    then
        condition=$(echo $line | grep -i "${start_str}")
        #echo "condition ============== $condition"     
        if [[ "$condition" != "" ]] && [[ "$condition" == *"SELECT"* ]];
        then
            start_flag="1"
            step=$[$step+1]
            content=${line}
        fi
    else
        if [ $step -eq 4 ]
        then
            content=${content},${line}
            step=$[$step+1]
        elif [ $step -eq 5 ]
        then
            content=${content},${line}
            # echo "content===$content"
            echo "${content}" >> $kudu_count_result_file
            start_flag=""
            content=""
            step=1
        else
            step=$[$step+1]
        fi
    fi

done < ${kudu_log_dir}kudu_count_query.log


import_test_data_result_file="${basedir}result/import_test_data_result.csv"

rm -rf $import_test_data_result_file

echo "generate ${basedir}result/import_test_data_result.csv..."

echo "SQL语句,执行时间,执行时长" >> $import_test_data_result_file

while read -r line || [[ -n ${line} ]]
do
    #echo "$line"
    if [[ "${start_flag}" == "" ]];
    then
        condition=$(echo $line | grep -i "${start_str}")
        #echo "condition ============== $condition"     
        if [[ "$condition" != "" ]] && [[ "$condition" == *"SELECT"* ]];
        then
            start_flag="1"
            step=$[$step+1]
	    sql=${line//,/，}
            content=${sql}
        fi
    else
        if [ $step -eq 2 ]
        then
            content=${content},${line}
            step=$[$step+1]
        elif [ $step -eq 4 ]
        then
            exec_time=${line//,/，}
            content=${content},${exec_time}
            #echo "content===$content"
            echo "${content}" >> $import_test_data_result_file
            start_flag=""
            content=""
            step=1
        else
            step=$[$step+1]
        fi
    fi

done < ${import_log_dir}import_test_data.log


kudu_analyze_result_file="${basedir}result/kudu_analyze_result.csv"

rm -rf $kudu_analyze_result_file

echo "generate ${basedir}result/kudu_analyze_result.csv..."

echo "SQL语句,更新内容,执行时长" >> $kudu_analyze_result_file

while read -r line || [[ -n ${line} ]]
do
    #echo "$line"
    if [[ "${start_flag}" == "" ]];
    then
        condition=$(echo $line | grep -i "${start_str}")
        #echo "condition ============== $condition"     
        if [[ "$condition" != "" ]] && [[ "$condition" == *"compute"* ]];
        then
            start_flag="1"
            step=$[$step+1]
            content=${line}
        fi
    else
        if [ $step -eq 2 ]
        then
            content=${content},${line}
            step=$[$step+1]
        elif [ $step -eq 3 ]
        then
            content=${content},${line}
            # echo "content===$content"
            echo "${content}" >> $kudu_analyze_result_file
            start_flag=""
            content=""
            step=1
        else
            step=$[$step+1]
        fi
    fi

done < ${kudu_log_dir}kudu_analyze.log
