使用TPC-DS对kudu进行基准测试

# 使用TPC-DS对kudu进行基准测试

目录

@[toc]
**使用TPC-DS工具对kudu列式存储数据库进行一个完整的自动化基准测试。**
**前置条件：CDH整套环境已经搭建好,登录Impala Daemon节点执行。**

操作步骤：
1.安装TPC-DS工具
2.初始化测试目录，使用TPC-DS工具生成测试数据dat文件和查询语句。
3.在HDFS新建目录，将测试数据dat文件上传到HDFS中。
4.将TPC-DS提供的DDL语句做语法兼容改造。
5.准备impala外部表和kudu表的DDL、COUNT、LIMIT语句，整理成SQL文件。准备统计分析（computer status table_name）的SQL语句。
6.调用start_tpc_import.sh进行impala外部表的建表，执行kudu内部表ddl，将impala外部表数据导入到kudu内部表，调用统计分析SQL，收集执行结果信息到tpc_ds_test/result目录查看，具体的SQL文件执行日志在tpc_ds_test/logs目录查看。
7.整理TPC-DS提供的SQL基准查询语句，由于有些语法不兼容kudu，需要进行手动调整，最后整理到单独SQL文件中。
8.调用start_kudu_query.sh，执行手动调整过后的TPC—DS的SQL基准查询。收集执行结果信息到tpc_ds_test/result目录查看，具体的SQL文件执行日志在tpc_ds_test/logs目录查看。

**所有脚本地址:https://github.com/linmeibao/tpc_ds_test**

## TPC-DS介绍

TPC-DS是一个面向决策支持系统(decision support system)的包含多维度常规应用模型的决策支持基准，包括查询(queries)与数据维护。此基准对被测系统(System Under Test's, SUT)在决策支持系统层面上的表现进行的评估具有代表性。

此基准体现决策支持系统以下特性：

1.测试大规模数据
2.对实际商业问题进行解答
3.执行需求多样或复杂的查询（如临时查询，报告，迭代OLAP，数据挖掘）
4.以高CPU和IO负载为特征
5.通过数据库维护对OLTP数据库资源进行周期同步
6.解决大数据问题，如关系型数据库(RDBMS)，或基于Hadoop/Spark的系统
    
基准结果用来测量，较为复杂的多用户决策中，单一用户模型下的查询响应时间，多用户模型下的查询吞吐量，以及数据维护表现。

TPC-DS采用星型、雪花型等多维数据模式。它包含7张事实表，17张纬度表平均每张表含有18列。其工作负载包含99个SQL查询，覆盖SQL99和2003的核心部分以及OLAP。这个测试集包含对大数据集的统计、报表生成、联机查询、数据挖掘等复杂应用，测试用的数据和值是有倾斜的，与真实数据一致。可以说TPC-DS是与真实场景非常接近的一个测试集，也是难度较大的一个测试集。

## kudu介绍
Kudu 是一个针对 Apache Hadoop 平台而开发的列式存储管理器。Kudu 共享 Hadoop 生态系统应用的常见技术特性: 它在 commodity hardware（商品硬件）上运行，horizontally scalable（水平可扩展），并支持 highly available（高可用）性操作。
 

## 安装TPC-DS工具

1.下载安装包，http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
。必须输入邮箱，他会发下载地址到邮箱中，点击下载地址即可下载。

2.在linux服务器进行解压：unzip 8fb68f0a-33f8-40be-96b4-322eb3e88af5-tpc-ds-tool.zip

```
drwxr-xr-x  10 hsy  staff   320B  5 25 16:13 v2.13.0rc1
➜ cd v2.13.0rc1 && ll
total 40
-rw-r--r--    1 hsy  staff    17K  5 25 15:27 EULA.txt
drwxr-xr-x  131 hsy  staff   4.1K  5 25 15:28 answer_sets
drwxr-xr-x  109 hsy  staff   3.4K  5 25 15:27 query_templates
drwxr-xr-x   17 hsy  staff   544B  5 25 15:27 query_variants
drwxr-xr-x    4 hsy  staff   128B  5 25 15:27 specification
drwxr-xr-x   70 hsy  staff   2.2K  5 25 15:27 tests
drwxr-xr-x  364 hsy  staff    11K  5 25 15:26 tools
```

3.安装TPC-DS编译依赖环境
```
yum -y install gcc gcc-c++ expect
```

4.进入tools目录进行编译：make

## 初始化目录，提前准备sh脚本，使用TPC-DS工具生成测试数据和基准查询语句

1.新建一个测试目录，新建tpc_test_init.sh，进行初始化目录，这里使用/root/目录
vim /root/tpc_test_init.sh
将👇的脚本复制到tpc_test_init.sh中
```
#!/bin/bash

# 防止误操作，先注释，手动更改一下测试目录，再把#basedir="/root/"的注释打开
#basedir="/root/"

cd $basedir
mkdir -p tpc_ds_test/{bin,logs,query_data,result,sql}
cd tpc_ds_test
mkdir -p logs/{ext,import,kudu,query}
mkdir -p sql/{ext,import,kudu,query}

touch ${basedir}tpc_ds_test/bin/batch_upload_to_hdfs.sh
touch ${basedir}tpc_ds_test/bin/start_tpc_import.sh
touch ${basedir}tpc_ds_test/bin/start_kudu_query.sh

chmod +x ${basedir}tpc_ds_test/bin/batch_upload_to_hdfs.sh
chmod +x ${basedir}tpc_ds_test/bin/start_tpc_import.sh
chmod +x ${basedir}tpc_ds_test/bin/start_kudu_query.sh

echo "Directory initialization is complete..."
```

2.执行初始化
chmod +x tpc_test_init.sh && sh tpc_test_init.sh

3.手动将TPC-DS的工具包移动到 /root/tpc_ds_test 下，解压
cp /root/8fb68f0a-33f8-40be-96b4-322eb3e88af5-tpc-ds-tool.zip  /root/tpc_ds_test
unzip /root/tpc_ds_test/8fb68f0a-33f8-40be-96b4-322eb3e88af5-tpc-ds-tool.zip


5.工具说明

dsdgen 生成数据
```
[root@test4 tools]# ./dsdgen -h
dsdgen Population Generator (Version 2.13.0)
Copyright Transaction Processing Performance Council (TPC) 2001 - 2020


USAGE: dsdgen [options]

Note: When defined in a parameter file (using -p), parmeters should
use the form below. Each option can also be set from the command
line, using a form of '-param [optional argument]'
Unique anchored substrings of options are also recognized, and 
case is ignored, so '-sc' is equivalent to '-SCALE'

General Options
===============
ABREVIATION =  <s>       -- build table with abreviation <s>
DIR =  <s>               -- generate tables in directory <s>
HELP =  <n>              -- display this message
PARAMS =  <s>            -- read parameters from file <s>
QUIET =  [Y|N]           -- disable all output to stdout/stderr
SCALE =  <n>             -- volume of data to generate in GB
TABLE =  <s>             -- build only table <s>
UPDATE =  <n>            -- generate update data set <n>
VERBOSE =  [Y|N]         -- enable verbose output
PARALLEL =  <n>          -- build data in <n> separate chunks
CHILD =  <n>             -- generate <n>th chunk of the parallelized data
RELEASE =  [Y|N]         -- display the release information
_FILTER =  [Y|N]         -- output data to stdout
VALIDATE =  [Y|N]        -- produce rows for data validation

Advanced Options
===============
DELIMITER =  <s>         -- use <s> as output field separator
DISTRIBUTIONS =  <s>     -- read distributions from file <s>
FORCE =  [Y|N]           -- over-write data files without prompting
SUFFIX =  <s>            -- use <s> as output file suffix
TERMINATE =  [Y|N]       -- end each record with a field delimiter
VCOUNT =  <n>            -- set number of validation rows to be produced
VSUFFIX =  <s>           -- set file suffix for data validation
RNGSEED =  <n>           -- set RNG seed
```

dsqgen 生成查询语句
```
[root@test4 tools]# ./dsqgen -h
qgen2 Population Generator (Version 2.13.0)
Copyright Transaction Processing Performance Council (TPC) 2001 - 2020


USAGE: qgen2 [options]

Note: When defined in a parameter file (using -p), parmeters should
use the form below. Each option can also be set from the command
line, using a form of '-param [optional argument]'
Unique anchored substrings of options are also recognized, and 
case is ignored, so '-sc' is equivalent to '-SCALE'

General Options
===============
FILE =  <s>              -- read parameters from file <s>
VERBOSE =  [Y|N]         -- enable verbose output
HELP =  [Y|N]            -- display this message
OUTPUT_DIR =  <s>        -- write query streams into directory <s>
QUIET =  [Y|N]           -- suppress all output (for scripting)
STREAMS =  <n>           -- generate <n> query streams/versions
INPUT =  <s>             -- read template names from <s>
SCALE =  <n>             -- assume a database of <n> GB
LOG =  <s>               -- write parameter log to <s>
QUALIFY =  [Y|N]         -- generate qualification queries in ascending order

Advanced Options
===============
DISTRIBUTIONS =  <s>     -- read distributions from file <s>
PATH_SEP =  <s>          -- use <s> to separate path elements
RNGSEED =  <n>           -- seed the RNG with <n>
RELEASE =  [Y|N]         -- display QGEN release info
TEMPLATE =  <s>          -- build queries from template <s> ONLY
COUNT =  <n>             -- generate <n> versions per stream (used with TEMPLATE)
DEBUG =  [Y|N]           -- minor debugging outptut
FILTER =  [Y|N]          -- write generated queries to stdout
DIALECT =  <s>           -- include query dialect defintions found in <s>.tpl
DIRECTORY =  <s>         -- look in <s> for templates
```


6.创建数据存放目录，可自行定义，这里使用/root/test_datas
mkdir /root/test_datas

7.生成测试数据

```
产生1G测试数据
cd /root/tpc_ds_test/v2.13.0rc1/tools && ./dsdgen -DIR /root/test_datas -SCALE 1 -TERMINATE N

并行产生1G数据
cd /root/tpc_ds_test/v2.13.0rc1/tools
./dsdgen -DIR /root/test_datas -SCALE 10 -parallel 4 -child 1
```

如果数据目录未创建（mkdir /root/test_datas）就执行生成数据命令，则会出现以下报错。所以需要将目录提前创建好。
```
[root@test4 tpc_ds_test]# cd /root/tpc_ds_test/v2.13.0rc1/tools && ./dsdgen -DIR /root/test_datas -SCALE 1 -TERMINATE N
dsdgen Population Generator (Version 2.13.0)
Copyright Transaction Processing Performance Council (TPC) 2001 - 2020
Warning: This scale factor is valid for QUALIFICATION ONLY
ERROR: Failed to open output file!
        File: print.c
        Line: 490
```

8.生成查询语句
- dialect这里选择netezza，SQL查询分页语法与要测试的kudu语句一致（可选：oracel、netezza、sqlserver、db2）netezza分页语法是limit所以这里选择它）
```
cd /root/tpc_ds_test/v2.13.0rc1/tools

./dsqgen -output_dir /root/tpc_ds_test/query_data -input ../query_templates/templates.lst -scale 1 -dialect netezza -directory ../query_templates
```

- /root/tpc_ds_test/query_data在目录初始化的时候已经生成了
- 查看/root/tpc_ds_test/query_data目录下生成的query_0.sql

## TPC—DS生成测试数据文件批量上传至HDFS

1.复制下面的脚本到 batch_upload_to_hdfs.sh 进行批量上传文件到HDFS
batch_upload_to_hdfs.sh在初始化目录时已经创建完成
cd /root/tpc_ds_test/bin && vim batch_upload_to_hdfs.sh

```
#!/bin/bash

#文件存放的目录
data_src_dir=/root/test_datas/

#文件上传到hdfs的根路径
hdfs_root_dir=/tmp/test/

file_dir=""

#创建测试数据目录
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
```

2.执行脚本
```
sh batch_upload_to_hdfs.sh
```

3.查看目录，是否上传成功
hdfs dfs -ls h /tmp/test/

```
[root@test4 bin]# hdfs dfs -ls h /tmp/test/
ls: `h': No such file or directory
Found 27 items
drwxr-xr-x   - root supergroup          0 2020-06-10 01:28 /tmp/test/call_center
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/catalog_page
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/catalog_returns
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/catalog_sales
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/customer
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/customer_address
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/customer_demographics
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/date_dim
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/dbgen_version
drwxr-xr-x   - root supergroup          0 2020-06-10 01:29 /tmp/test/handled
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/handled_data
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/household_demographics
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/income_band
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/inventory
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/item
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/promotion
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/reason
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/ship_mode
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/store
drwxr-xr-x   - root supergroup          0 2020-06-10 01:30 /tmp/test/store_returns
drwxr-xr-x   - root supergroup          0 2020-06-10 01:31 /tmp/test/store_sales
drwxr-xr-x   - root supergroup          0 2020-06-10 01:31 /tmp/test/time_dim
drwxr-xr-x   - root supergroup          0 2020-06-10 01:31 /tmp/test/warehouse
drwxr-xr-x   - root supergroup          0 2020-06-10 01:31 /tmp/test/web_page
drwxr-xr-x   - root supergroup          0 2020-06-10 01:31 /tmp/test/web_returns
drwxr-xr-x   - root supergroup          0 2020-06-10 01:31 /tmp/test/web_sales
drwxr-xr-x   - root supergroup          0 2020-06-10 01:31 /tmp/test/web_site
[root@test4 bin]# hdfs dfs -ls h /tmp/test/call_center
ls: `h': No such file or directory
Found 1 items
-rw-r--r--   3 root supergroup       1891 2020-06-10 01:28 /tmp/test/call_center/call_center.dat
[root@test4 bin]# hdfs dfs -ls h /tmp/test/catalog_page
ls: `h': No such file or directory
Found 1 items
-rw-r--r--   3 root supergroup    1631792 2020-06-10 01:29 /tmp/test/catalog_page/catalog_page.dat
[root@test4 bin]# 
```

## TPC-DS提供DDL语句与impala和kudu建表语法有差异，需要手动改造

TPC-DS已经提前准备好了创建表相关的SQL文件，文件位于tools目录下。
但是由于各种数据库之间语法差异，无法直接使用，需要手动改造。
	- tpcds.sql 创建25张表的sql语句
	- tpcds_ri.sql 创建表与表之间关系的sql语句
	- tpcds_source.sql
	- 主要是用到tpcds.sql文件

测试数据已经导入到HDFS，可以开始创建impala外部表，随后使用“INSERT INTO kudu表 SELECT * FROM 外部表”的方式，将测试数据导入到kudu当中。

1.以下为改造后兼容impala语法的外部表DDL
```
use tpc_test;

DROP TABLE IF EXISTS ext_dbgen_version;
create external table ext_dbgen_version
(
    dv_version                varchar(30)                   ,
    dv_create_date            TIMESTAMP                          ,
    dv_create_time            varchar(30)                          ,
    dv_cmdline_args           varchar(200)                  
)row format delimited fields terminated by '|'
location '/tmp/test/dbgen_version';


DROP TABLE IF EXISTS ext_customer_address;
create external table ext_customer_address
(
    ca_address_sk             integer               ,
    ca_address_id             char(16)              ,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)                      
)row format delimited fields terminated by '|'
location '/tmp/test/customer_address';


DROP TABLE IF EXISTS ext_customer_demographics;
create external table ext_customer_demographics
(
    cd_demo_sk                integer               ,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       
)row format delimited fields terminated by '|'
location '/tmp/test/customer_demographics';

DROP TABLE IF EXISTS ext_date_dim;
create external table ext_date_dim
(
    d_date_sk                 integer               ,
    d_date_id                 char(16)              ,
    d_date                    TIMESTAMP                          ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)                       
)row format delimited fields terminated by '|'
location '/tmp/test/date_dim';


DROP TABLE IF EXISTS ext_warehouse;
create external table ext_warehouse
(
    w_warehouse_sk            integer               ,
    w_warehouse_id            char(16)              ,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/warehouse';


DROP TABLE IF EXISTS ext_ship_mode;
create external table ext_ship_mode
(
    sm_ship_mode_sk           integer               ,
    sm_ship_mode_id           char(16)              ,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      
)row format delimited fields terminated by '|'
location '/tmp/test/ship_mode';


DROP TABLE IF EXISTS ext_time_dim;
create external table ext_time_dim
(
    t_time_sk                 integer                       ,
    t_time_id                 char(16)                      ,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      
)row format delimited fields terminated by '|'
location '/tmp/test/time_dim';


DROP TABLE IF EXISTS ext_reason;
create external table ext_reason
(
    r_reason_sk               integer               ,
    r_reason_id               char(16)              ,
    r_reason_desc             char(100)              
)row format delimited fields terminated by '|'
location '/tmp/test/reason';


DROP TABLE IF EXISTS ext_income_band;
create external table ext_income_band
(
    ib_income_band_sk         integer               ,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
)row format delimited fields terminated by '|'
location '/tmp/test/income_band';


DROP TABLE IF EXISTS ext_item;
create external table ext_item
(
    i_item_sk                 integer                       ,
    i_item_id                 char(16)                      ,
    i_rec_start_date         TIMESTAMP                         ,
    i_rec_end_date           TIMESTAMP                         ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   char(50)                      ,
    i_class_id                integer                       ,
    i_class                   char(50)                      ,
    i_category_id             integer                       ,
    i_category                char(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            char(50)                      
)row format delimited fields terminated by '|'
location '/tmp/test/item';


DROP TABLE IF EXISTS ext_store;
create external table ext_store
(
    s_store_sk                integer                       ,
    s_store_id                char(16)                      ,
    s_rec_start_date         TIMESTAMP                         ,
    s_rec_end_date           TIMESTAMP                         ,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/store';


DROP TABLE IF EXISTS ext_call_center;
create external table ext_call_center
(
    cc_call_center_sk         integer               ,
    cc_call_center_id         char(16)              ,
    cc_rec_start_date        TIMESTAMP                         ,
    cc_rec_end_date          TIMESTAMP                         ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/call_center';


DROP TABLE IF EXISTS ext_customer;
create external table ext_customer
(
    c_customer_sk             integer               ,
    c_customer_id             char(16)              ,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      
)row format delimited fields terminated by '|'
location '/tmp/test/customer';


DROP TABLE IF EXISTS ext_web_site;
create external table ext_web_site
(
    web_site_sk               integer               ,
    web_site_id               char(16)              ,
    web_rec_start_date       TIMESTAMP                         ,
    web_rec_end_date         TIMESTAMP                         ,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/web_site';

DROP TABLE IF EXISTS ext_store_returns;
create external table ext_store_returns
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               ,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          integer               ,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/store_returns';

DROP TABLE IF EXISTS ext_household_demographics;
create external table ext_household_demographics
(
    hd_demo_sk                integer               ,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
)row format delimited fields terminated by '|'
location '/tmp/test/household_demographics';

DROP TABLE IF EXISTS ext_web_page;
create external table ext_web_page
(
    wp_web_page_sk            integer               ,
    wp_web_page_id            char(16)              ,
    wp_rec_start_date        TIMESTAMP                         ,
    wp_rec_end_date          TIMESTAMP                         ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       
)row format delimited fields terminated by '|'
location '/tmp/test/web_page';

DROP TABLE IF EXISTS ext_promotion;
create external table ext_promotion
(
    p_promo_sk                integer               ,
    p_promo_id                char(16)              ,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                        
)row format delimited fields terminated by '|'
location '/tmp/test/promotion';

DROP TABLE IF EXISTS ext_catalog_page;
create external table ext_catalog_page
(
    cp_catalog_page_sk        integer               ,
    cp_catalog_page_id        char(16)              ,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  
)row format delimited fields terminated by '|'
location '/tmp/test/catalog_page';

DROP TABLE IF EXISTS ext_inventory;
create external table ext_inventory
(
    inv_date_sk               integer               ,
    inv_item_sk               integer               ,
    inv_warehouse_sk          integer               ,
    inv_quantity_on_hand      integer                           
)row format delimited fields terminated by '|'
location '/tmp/test/inventory';


DROP TABLE IF EXISTS ext_catalog_returns;
create external table ext_catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               ,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           integer               ,
    cr_return_quantity        integer                       ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/catalog_returns';

DROP TABLE IF EXISTS ext_web_returns;
create external table ext_web_returns
(
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               ,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           integer               ,
    wr_return_quantity        integer                       ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/web_returns';

DROP TABLE IF EXISTS ext_web_sales;
create external table ext_web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               ,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           integer               ,
    ws_quantity               integer                       ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/web_sales';


DROP TABLE IF EXISTS ext_catalog_sales;
create external table ext_catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               ,
    cs_promo_sk               integer                       ,
    cs_order_number           integer               ,
    cs_quantity               integer                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/catalog_sales';

DROP TABLE IF EXISTS ext_store_sales;
create external table ext_store_sales
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               ,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          integer               ,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  
)row format delimited fields terminated by '|'
location '/tmp/test/store_sales';
```


2.以下为改造后兼容impala语法的kudu表DDL（暂时默认每个表都是3个分区，分区键为主键）
```
use tpc_test;

DROP TABLE IF EXISTS dbgen_version;
create table dbgen_version
(
    dv_version                STRING                   ,
    dv_create_date            TIMESTAMP                          ,
    dv_create_time            STRING                          ,
    dv_cmdline_args           STRING                  ,
    PRIMARY KEY(dv_version)
) 
PARTITION BY HASH(dv_version) PARTITIONS 3
STORED AS KUDU
;



DROP TABLE IF EXISTS customer_address;
create table customer_address
(
    ca_address_sk             integer                       ,
    ca_address_id            STRING                     ,
    ca_street_number         STRING                     ,
    ca_street_name           STRING                  ,
    ca_street_type           STRING                     ,
    ca_suite_number          STRING                     ,
    ca_city                  STRING                  ,
    ca_county                STRING                  ,
    ca_state                 STRING                     ,
    ca_zip                   STRING                     ,
    ca_country               STRING                  ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type         STRING                     ,
    primary key (ca_address_sk)
)
PARTITION BY HASH(ca_address_sk) PARTITIONS 3
STORED AS KUDU
;



DROP TABLE IF EXISTS customer_demographics;
create table customer_demographics
(
    cd_demo_sk                integer                       ,
    cd_gender                STRING                     ,
    cd_marital_status        STRING                     ,
    cd_education_status      STRING                     ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating         STRING                     ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       ,
    primary key (cd_demo_sk)
)
PARTITION BY HASH(cd_demo_sk) PARTITIONS 3
STORED AS KUDU
;



DROP TABLE IF EXISTS date_dim;
create table date_dim
(
    d_date_sk                 integer                       ,
    d_date_id                STRING                     ,
    d_date                   TIMESTAMP                         ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name               STRING                     ,
    d_quarter_name           STRING                     ,
    d_holiday                STRING                     ,
    d_weekend                STRING                     ,
    d_following_holiday      STRING                     ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day            STRING                     ,
    d_current_week           STRING                     ,
    d_current_month          STRING                     ,
    d_current_quarter        STRING                     ,
    d_current_year           STRING                     ,
    primary key (d_date_sk)
)
PARTITION BY HASH(d_date_sk) PARTITIONS 3
STORED AS KUDU
;



DROP TABLE IF EXISTS warehouse;
create table warehouse
(
    w_warehouse_sk            integer                       ,
    w_warehouse_id           STRING                     ,
    w_warehouse_name         STRING                  ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number          STRING                     ,
    w_street_name            STRING                  ,
    w_street_type            STRING                     ,
    w_suite_number           STRING                     ,
    w_city                   STRING                  ,
    w_county                 STRING                  ,
    w_state                  STRING                     ,
    w_zip                    STRING                     ,
    w_country                STRING                  ,
    w_gmt_offset              decimal(5,2)                  ,
    primary key (w_warehouse_sk)
)
PARTITION BY HASH(w_warehouse_sk) PARTITIONS 3
STORED AS KUDU
;




DROP TABLE IF EXISTS ship_mode;
create table ship_mode
(
    sm_ship_mode_sk           integer                       ,
    sm_ship_mode_id          STRING                     ,
    sm_type                  STRING                     ,
    sm_code                  STRING                     ,
    sm_carrier               STRING                     ,
    sm_contract              STRING                     ,
    primary key (sm_ship_mode_sk)
)
PARTITION BY HASH(sm_ship_mode_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS time_dim;
create table time_dim
(
    t_time_sk                 integer                       ,
    t_time_id                STRING                     ,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                  STRING                     ,
    t_shift                  STRING                     ,
    t_sub_shift              STRING                     ,
    t_meal_time              STRING                     ,
    primary key (t_time_sk)
)
PARTITION BY HASH(t_time_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS reason;
create table reason
(
    r_reason_sk               integer                       ,
    r_reason_id              STRING                     ,
    r_reason_desc            STRING                     ,
    primary key (r_reason_sk)
)
PARTITION BY HASH(r_reason_sk) PARTITIONS 3
STORED AS KUDU
;

DROP TABLE IF EXISTS income_band;
create table income_band
(
    ib_income_band_sk         integer                       ,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       ,
    primary key (ib_income_band_sk)
)
PARTITION BY HASH(ib_income_band_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS item;
create table item
(
    i_item_sk                 integer                       ,
    i_item_id                STRING                     ,
    i_rec_start_date         TIMESTAMP                         ,
    i_rec_end_date           TIMESTAMP                         ,
    i_item_desc              STRING                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                  STRING                     ,
    i_class_id                integer                       ,
    i_class                  STRING                     ,
    i_category_id             integer                       ,
    i_category               STRING                     ,
    i_manufact_id             integer                       ,
    i_manufact               STRING                     ,
    i_size                   STRING                     ,
    i_formulation            STRING                     ,
    i_color                  STRING                     ,
    i_units                  STRING                     ,
    i_container              STRING                     ,
    i_manager_id              integer                       ,
    i_product_name           STRING                     ,
    primary key (i_item_sk)
)
PARTITION BY HASH(i_item_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS store;
create table store
(
    s_store_sk                integer                       ,
    s_store_id               STRING                     ,
    s_rec_start_date         TIMESTAMP                         ,
    s_rec_end_date           TIMESTAMP                         ,
    s_closed_date_sk          integer                       ,
    s_store_name             STRING                  ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                  STRING                     ,
    s_manager                STRING                  ,
    s_market_id               integer                       ,
    s_geography_class        STRING                  ,
    s_market_desc            STRING                  ,
    s_market_manager         STRING                  ,
    s_division_id             integer                       ,
    s_division_name          STRING                  ,
    s_company_id              integer                       ,
    s_company_name           STRING                  ,
    s_street_number          STRING                  ,
    s_street_name            STRING                  ,
    s_street_type            STRING                     ,
    s_suite_number           STRING                     ,
    s_city                   STRING                  ,
    s_county                 STRING                  ,
    s_state                  STRING                     ,
    s_zip                    STRING                     ,
    s_country                STRING                  ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  ,
    primary key (s_store_sk)
)
PARTITION BY HASH(s_store_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS call_center;
create table call_center
(
    cc_call_center_sk         integer                       ,
    cc_call_center_id        STRING                     ,
    cc_rec_start_date        TIMESTAMP                         ,
    cc_rec_end_date          TIMESTAMP                         ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                  STRING                  ,
    cc_class                 STRING                  ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                 STRING                     ,
    cc_manager               STRING                  ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class             STRING                     ,
    cc_mkt_desc              STRING                  ,
    cc_market_manager        STRING                  ,
    cc_division               integer                       ,
    cc_division_name         STRING                  ,
    cc_company                integer                       ,
    cc_company_name          STRING                     ,
    cc_street_number         STRING                     ,
    cc_street_name           STRING                  ,
    cc_street_type           STRING                     ,
    cc_suite_number          STRING                     ,
    cc_city                  STRING                  ,
    cc_county                STRING                  ,
    cc_state                 STRING                     ,
    cc_zip                   STRING                     ,
    cc_country               STRING                  ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  ,
    primary key (cc_call_center_sk)           
)
PARTITION BY HASH(cc_call_center_sk) PARTITIONS 3
STORED AS KUDU
;

DROP TABLE IF EXISTS customer;
create table customer
(
    c_customer_sk             integer                       ,
    c_customer_id            STRING                     ,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation             STRING                     ,
    c_first_name             STRING                     ,
    c_last_name              STRING                     ,
    c_preferred_cust_flag    STRING                     ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country          STRING                  ,
    c_login                  STRING                     ,
    c_email_address          STRING                     ,
    c_last_review_date       STRING                     ,
    primary key (c_customer_sk)
)
PARTITION BY HASH(c_customer_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS web_site;
create table web_site
(
    web_site_sk               integer                       ,
    web_site_id              STRING                     ,
    web_rec_start_date       TIMESTAMP                         ,
    web_rec_end_date         TIMESTAMP                         ,
    web_name                 STRING                  ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                STRING                  ,
    web_manager              STRING                  ,
    web_mkt_id                integer                       ,
    web_mkt_class            STRING                  ,
    web_mkt_desc             STRING                  ,
    web_market_manager       STRING                  ,
    web_company_id            integer                       ,
    web_company_name         STRING                     ,
    web_street_number        STRING                     ,
    web_street_name          STRING                  ,
    web_street_type          STRING                     ,
    web_suite_number         STRING                     ,
    web_city                 STRING                  ,
    web_county               STRING                  ,
    web_state                STRING                     ,
    web_zip                  STRING                     ,
    web_country              STRING                  ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  ,
    primary key (web_site_sk)
)
PARTITION BY HASH(web_site_sk) PARTITIONS 3
STORED AS KUDU
;


--ERROR: ImpalaRuntimeException: Error creating Kudu table 'impala::tpc_test.store_returns'
--CAUSED BY: ImpalaRuntimeException: Kudu PRIMARY KEY columns must be specified as the first columns in the table (expected leading columns ('sr_item_sk', 'sr_ticket_number') but found ('sr_item_sk', 'sr_returned_date_sk'))
-- 字段顺序有要求

DROP TABLE IF EXISTS store_returns;
create table store_returns
(
    sr_item_sk                integer                       ,
    sr_ticket_number          integer                       ,
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  ,
    primary key (sr_item_sk, sr_ticket_number)
)
PARTITION BY HASH(sr_item_sk,sr_ticket_number) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS household_demographics;
create table household_demographics
(
    hd_demo_sk                integer                       ,
    hd_income_band_sk         integer                       ,
    hd_buy_potential         STRING                     ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       ,
    primary key (hd_demo_sk)
)
PARTITION BY HASH(hd_demo_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS web_page;
create table web_page
(
    wp_web_page_sk            integer                       ,
    wp_web_page_id           STRING                     ,
    wp_rec_start_date        TIMESTAMP                         ,
    wp_rec_end_date          TIMESTAMP                         ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag          STRING                     ,
    wp_customer_sk            integer                       ,
    wp_url                   STRING                  ,
    wp_type                  STRING                     ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       ,
    primary key (wp_web_page_sk)
)
PARTITION BY HASH(wp_web_page_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS promotion;
create table promotion
(
    p_promo_sk                integer                       ,
    p_promo_id               STRING                     ,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name             STRING                     ,
    p_channel_dmail          STRING                     ,
    p_channel_email          STRING                     ,
    p_channel_catalog        STRING                     ,
    p_channel_tv             STRING                     ,
    p_channel_radio          STRING                     ,
    p_channel_press          STRING                     ,
    p_channel_event          STRING                     ,
    p_channel_demo           STRING                     ,
    p_channel_details        STRING                  ,
    p_purpose                STRING                     ,
    p_discount_active        STRING                     ,
    primary key (p_promo_sk)
)
PARTITION BY HASH(p_promo_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS catalog_page;
create table catalog_page
(
    cp_catalog_page_sk        integer                       ,
    cp_catalog_page_id       STRING                     ,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department            STRING                  ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description           STRING                  ,
    cp_type                  STRING                  ,
    primary key (cp_catalog_page_sk)
)
PARTITION BY HASH(cp_catalog_page_sk) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS inventory;
create table inventory
(
    inv_date_sk               integer                       ,
    inv_item_sk               integer                       ,
    inv_warehouse_sk          integer                       ,
    inv_quantity_on_hand      integer                       ,
    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
)
PARTITION BY HASH(inv_date_sk, inv_item_sk, inv_warehouse_sk) PARTITIONS 3
STORED AS KUDU
;



DROP TABLE IF EXISTS catalog_returns;
create table catalog_returns
(
    cr_item_sk                integer               ,
    cr_order_number           integer               ,
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_return_quantity        integer                       ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)                  ,
    primary key (cr_item_sk, cr_order_number)
)
PARTITION BY HASH(cr_item_sk, cr_order_number) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS web_returns;
create table web_returns
(    
    wr_item_sk                integer               ,
    wr_order_number           integer               ,
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_return_quantity        integer                       ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)                  ,

    primary key (wr_item_sk, wr_order_number)
)
PARTITION BY HASH(wr_item_sk, wr_order_number) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS web_sales;
create table web_sales
(
    ws_item_sk                integer               ,
    ws_order_number           integer               ,
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_quantity               integer                       ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)                  ,
    primary key (ws_item_sk, ws_order_number)
)
PARTITION BY HASH(ws_item_sk, ws_order_number) PARTITIONS 3
STORED AS KUDU
;


DROP TABLE IF EXISTS catalog_sales;
create table catalog_sales
(
    cs_item_sk                integer               ,
    cs_order_number           integer               ,
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_promo_sk               integer                       ,
    cs_quantity               integer                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  ,
    primary key (cs_item_sk, cs_order_number)
)
PARTITION BY HASH(cs_item_sk, cs_order_number) PARTITIONS 3
STORED AS KUDU
;

DROP TABLE IF EXISTS store_sales;
create table store_sales
(
    ss_item_sk                integer               ,
    ss_ticket_number          integer               ,
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)
)
PARTITION BY HASH(ss_item_sk, ss_ticket_number) PARTITIONS 3
STORED AS KUDU
;
```

## impala外部表和kudu表的创建-外部表数据导入到kudu表中

1.进入impala-shell窗口，先新建tpc_test数据库：create database tpc_test;
```
➜ impala-shell
Starting Impala Shell without Kerberos authentication
Connected to test2:21000
Server version: impala version 2.8.0-cdh5.11.2 RELEASE (build f89269c4b96da14a841e94bdf6d4d48821b0d658)
***********************************************************************************
Welcome to the Impala shell.
(Impala Shell v2.8.0-cdh5.11.2 (f89269c) built on Fri Aug 18 14:04:44 PDT 2017)

The HISTORY command lists all shell commands in chronological order.
***********************************************************************************
[test2:21000] > show databases;
Query: show databases
+------------------+----------------------------------------------+
| name             | comment                                      |
+------------------+----------------------------------------------+
| _impala_builtins | System database for Impala builtin functions |
| default          | Default Hive database                        |
+------------------+----------------------------------------------+
Fetched 2 row(s) in 0.01s
[test4:21000] default> create database tpc_test;
Query: create database tpc_test
+----------------------------+
| summary                    |
+----------------------------+
| Database has been created. |
+----------------------------+
Fetched 1 row(s) in 0.40s
[test4:21000] default> 
```

2.新建外部表ddl文件
vim /root/tpc_ds_test/sql/ext/ext_ddl.sql
将上面👆的外部表DDL复制进去

3.新建kudu表ddl文件
vim /root/tpc_ds_test/sql/kudu/kudu_ddl.sql
将上面👆的kudu表DDL复制进去

4.新建简易COUNT查询SQL
vim /root/tpc_ds_test/sql/ext/ext_count_query.sql
将下面👇的SQL复制进去
```
use tpc_test;

SELECT COUNT(1) FROM ext_call_center           ;
SELECT COUNT(1) FROM ext_catalog_page          ;
SELECT COUNT(1) FROM ext_catalog_returns       ;
SELECT COUNT(1) FROM ext_catalog_sales         ;
SELECT COUNT(1) FROM ext_customer              ;
SELECT COUNT(1) FROM ext_customer_address      ;
SELECT COUNT(1) FROM ext_customer_demographics ;
SELECT COUNT(1) FROM ext_date_dim              ;
SELECT COUNT(1) FROM ext_dbgen_version         ;
SELECT COUNT(1) FROM ext_household_demographics;
SELECT COUNT(1) FROM ext_income_band           ;
SELECT COUNT(1) FROM ext_inventory             ;
SELECT COUNT(1) FROM ext_item                  ;
SELECT COUNT(1) FROM ext_promotion             ;
SELECT COUNT(1) FROM ext_reason                ;
SELECT COUNT(1) FROM ext_ship_mode             ;
SELECT COUNT(1) FROM ext_store                 ;
SELECT COUNT(1) FROM ext_store_returns         ;
SELECT COUNT(1) FROM ext_store_sales           ;
SELECT COUNT(1) FROM ext_time_dim              ;
SELECT COUNT(1) FROM ext_warehouse             ;
SELECT COUNT(1) FROM ext_web_page              ;
SELECT COUNT(1) FROM ext_web_returns           ;
SELECT COUNT(1) FROM ext_web_sales             ;
SELECT COUNT(1) FROM ext_web_site              ;
```

5.新建简易limit查询SQL，主要验证外部表数据是否能够被正常查询
vim /root/tpc_ds_test/sql/ext/ext_limit_query.sql
将下面👇的SQL复制进去

```
use tpc_test;

SELECT * FROM ext_call_center           limit 2;
SELECT * FROM ext_catalog_page          limit 2;
SELECT * FROM ext_catalog_returns       limit 2;
SELECT * FROM ext_catalog_sales         limit 2;
SELECT * FROM ext_customer              limit 2;
SELECT * FROM ext_customer_address      limit 2;
SELECT * FROM ext_customer_demographics limit 2;
SELECT * FROM ext_date_dim              limit 2;
SELECT * FROM ext_dbgen_version         limit 2;
SELECT * FROM ext_household_demographics limit 2;
SELECT * FROM ext_income_band           limit 2;
SELECT * FROM ext_inventory             limit 2;
SELECT * FROM ext_item                  limit 2;
SELECT * FROM ext_promotion             limit 2;
SELECT * FROM ext_reason                limit 2;
SELECT * FROM ext_ship_mode             limit 2;
SELECT * FROM ext_store                 limit 2;
SELECT * FROM ext_store_returns         limit 2;
SELECT * FROM ext_store_sales           limit 2;
SELECT * FROM ext_time_dim              limit 2;
SELECT * FROM ext_warehouse             limit 2;
SELECT * FROM ext_web_page              limit 2;
SELECT * FROM ext_web_returns           limit 2;
SELECT * FROM ext_web_sales             limit 2;
SELECT * FROM ext_web_site              limit 2;
```


6.准备导数SQL
vim /root/tpc_ds_test/sql/import/import_test_data.sql
将下面👇的SQL复制进去
```
use tpc_test;
INSERT INTO call_center            SELECT * FROM ext_call_center           ;
INSERT INTO catalog_page           SELECT * FROM ext_catalog_page          ;
INSERT INTO catalog_returns        SELECT     cr_item_sk                              ,    cr_order_number                          ,    cr_returned_date_sk                              ,    cr_returned_time_sk                              ,    cr_refunded_customer_sk                          ,    cr_refunded_cdemo_sk                             ,    cr_refunded_hdemo_sk                             ,    cr_refunded_addr_sk                              ,    cr_returning_customer_sk                         ,    cr_returning_cdemo_sk                            ,    cr_returning_hdemo_sk                            ,    cr_returning_addr_sk                             ,    cr_call_center_sk                                ,    cr_catalog_page_sk                               ,    cr_ship_mode_sk                                  ,    cr_warehouse_sk                                  ,    cr_reason_sk                                     ,    cr_return_quantity                               ,    cr_return_amount                            ,    cr_return_tax                               ,    cr_return_amt_inc_tax                       ,    cr_fee                                      ,    cr_return_ship_cost                         ,    cr_refunded_cash                            ,    cr_reversed_charge                          ,    cr_store_credit                             ,    cr_net_loss                                  FROM ext_catalog_returns;
INSERT INTO catalog_sales          SELECT     cs_item_sk                              ,    cs_order_number                          ,    cs_sold_date_sk                                  ,    cs_sold_time_sk                                  ,    cs_ship_date_sk                                  ,    cs_bill_customer_sk                              ,    cs_bill_cdemo_sk                                 ,    cs_bill_hdemo_sk                                 ,    cs_bill_addr_sk                                  ,    cs_ship_customer_sk                              ,    cs_ship_cdemo_sk                                 ,    cs_ship_hdemo_sk                                 ,    cs_ship_addr_sk                                  ,    cs_call_center_sk                                ,    cs_catalog_page_sk                               ,    cs_ship_mode_sk                                  ,    cs_warehouse_sk                                  ,    cs_promo_sk                                      ,    cs_quantity                                      ,    cs_wholesale_cost                           ,    cs_list_price                               ,    cs_sales_price                              ,    cs_ext_discount_amt                         ,    cs_ext_sales_price                          ,    cs_ext_wholesale_cost                       ,    cs_ext_list_price                           ,    cs_ext_tax                                  ,    cs_coupon_amt                               ,    cs_ext_ship_cost                            ,    cs_net_paid                                 ,    cs_net_paid_inc_tax                         ,    cs_net_paid_inc_ship                        ,    cs_net_paid_inc_ship_tax                    ,    cs_net_profit                               FROM ext_catalog_sales;
INSERT INTO customer               SELECT * FROM ext_customer              ;
INSERT INTO customer_address       SELECT * FROM ext_customer_address      ;
INSERT INTO customer_demographics  SELECT * FROM ext_customer_demographics ;
INSERT INTO date_dim               SELECT * FROM ext_date_dim              ;
INSERT INTO dbgen_version          SELECT * FROM ext_dbgen_version         ;
INSERT INTO household_demographics SELECT * FROM ext_household_demographics;
INSERT INTO income_band            SELECT * FROM ext_income_band           ;
INSERT INTO inventory              SELECT * FROM ext_inventory             ;
INSERT INTO item                   SELECT * FROM ext_item                  ;
INSERT INTO promotion              SELECT * FROM ext_promotion             ;
INSERT INTO reason                 SELECT * FROM ext_reason                ;
INSERT INTO ship_mode              SELECT * FROM ext_ship_mode             ;
INSERT INTO store                  SELECT * FROM ext_store                 ;
INSERT INTO store_returns          SELECT     sr_item_sk                               ,    sr_ticket_number                         ,    sr_returned_date_sk                              ,    sr_return_time_sk                                ,    sr_customer_sk                                   ,    sr_cdemo_sk                                      ,    sr_hdemo_sk                                      ,    sr_addr_sk                                       ,    sr_store_sk                                      ,    sr_reason_sk                                     ,    sr_return_quantity                               ,    sr_return_amt                               ,    sr_return_tax                               ,    sr_return_amt_inc_tax                       ,    sr_fee                                      ,    sr_return_ship_cost                         ,    sr_refunded_cash                            ,    sr_reversed_charge                          ,    sr_store_credit                             ,    sr_net_loss                                 FROM ext_store_returns;
INSERT INTO store_sales            SELECT     ss_item_sk                               ,    ss_ticket_number                         ,    ss_sold_date_sk                                 ,    ss_sold_time_sk                                  ,    ss_customer_sk                                   ,    ss_cdemo_sk                                      ,    ss_hdemo_sk                                      ,    ss_addr_sk                                       ,    ss_store_sk                                      ,    ss_promo_sk                                      ,    ss_quantity                                      ,    ss_wholesale_cost                           ,    ss_list_price                               ,    ss_sales_price                              ,    ss_ext_discount_amt                         ,    ss_ext_sales_price                          ,    ss_ext_wholesale_cost                       ,    ss_ext_list_price                           ,    ss_ext_tax                                  ,    ss_coupon_amt                               ,    ss_net_paid                                 ,    ss_net_paid_inc_tax                         ,    ss_net_profit                               FROM ext_store_sales;
INSERT INTO time_dim               SELECT * FROM ext_time_dim              ;
INSERT INTO warehouse              SELECT * FROM ext_warehouse             ;
INSERT INTO web_page               SELECT * FROM ext_web_page              ;
INSERT INTO web_returns            SELECT     wr_item_sk                               ,    wr_order_number                          ,    wr_returned_date_sk                              ,    wr_returned_time_sk                              ,    wr_refunded_customer_sk                          ,    wr_refunded_cdemo_sk                             ,    wr_refunded_hdemo_sk                             ,    wr_refunded_addr_sk                              ,    wr_returning_customer_sk                         ,    wr_returning_cdemo_sk                            ,    wr_returning_hdemo_sk                            ,    wr_returning_addr_sk                             ,    wr_web_page_sk                                   ,    wr_reason_sk                                     ,    wr_return_quantity                               ,    wr_return_amt                               ,    wr_return_tax                               ,    wr_return_amt_inc_tax                       ,    wr_fee                                      ,    wr_return_ship_cost                         ,    wr_refunded_cash                            ,    wr_reversed_charge                          ,    wr_account_credit                           ,    wr_net_loss                                 FROM ext_web_returns;
INSERT INTO web_sales              SELECT     ws_item_sk                               ,    ws_order_number                          ,    ws_sold_date_sk                                  ,    ws_sold_time_sk                                  ,    ws_ship_date_sk                                  ,    ws_bill_customer_sk                              ,    ws_bill_cdemo_sk                                 ,    ws_bill_hdemo_sk                                 ,    ws_bill_addr_sk                                  ,    ws_ship_customer_sk                              ,    ws_ship_cdemo_sk                                 ,    ws_ship_hdemo_sk                                 ,    ws_ship_addr_sk                                  ,    ws_web_page_sk                                   ,    ws_web_site_sk                                   ,    ws_ship_mode_sk                                  ,    ws_warehouse_sk                                  ,    ws_promo_sk                                      ,    ws_quantity                                      ,    ws_wholesale_cost                           ,    ws_list_price                               ,    ws_sales_price                              ,    ws_ext_discount_amt                         ,    ws_ext_sales_price                          ,    ws_ext_wholesale_cost                       ,    ws_ext_list_price                           ,    ws_ext_tax                                  ,    ws_coupon_amt                               ,    ws_ext_ship_cost                            ,    ws_net_paid                                 ,    ws_net_paid_inc_tax                         ,    ws_net_paid_inc_ship                        ,    ws_net_paid_inc_ship_tax                    ,    ws_net_profit                               FROM ext_web_sales;
INSERT INTO web_site               SELECT * FROM ext_web_site              ;

```

7.创建kudu_analyze.sql，用于统计分析Impala|kudu的表
vim /root/tpc_ds_test/sql/kudu/kudu_analyze.sql
```
use tpc_test;
compute stats call_center ;
compute stats catalog_page ;
compute stats catalog_returns ;
compute stats catalog_sales ;
compute stats customer_address ;
compute stats customer_demographics ;
compute stats customer ;
compute stats date_dim ;
compute stats household_demographics ;
compute stats income_band ;
compute stats inventory ;
compute stats item ;
compute stats promotion ;
compute stats reason ;
compute stats ship_mode ;
compute stats store_returns ;
compute stats store_sales ;
compute stats store ;
compute stats time_dim ;
compute stats warehouse ;
compute stats web_page ;
compute stats web_returns ;
compute stats web_sales ;
compute stats web_site ;
```

8.新建简易COUNT查询SQL
vim /root/tpc_ds_test/sql/kudu/kudu_count_query.sql
将下面👇的SQL复制进去
```
use tpc_test;

SELECT COUNT(1) FROM call_center           ;
SELECT COUNT(1) FROM catalog_page          ;
SELECT COUNT(1) FROM catalog_returns       ;
SELECT COUNT(1) FROM catalog_sales         ;
SELECT COUNT(1) FROM customer              ;
SELECT COUNT(1) FROM customer_address      ;
SELECT COUNT(1) FROM customer_demographics ;
SELECT COUNT(1) FROM date_dim              ;
SELECT COUNT(1) FROM dbgen_version         ;
SELECT COUNT(1) FROM household_demographics;
SELECT COUNT(1) FROM income_band           ;
SELECT COUNT(1) FROM inventory             ;
SELECT COUNT(1) FROM item                  ;
SELECT COUNT(1) FROM promotion             ;
SELECT COUNT(1) FROM reason                ;
SELECT COUNT(1) FROM ship_mode             ;
SELECT COUNT(1) FROM store                 ;
SELECT COUNT(1) FROM store_returns         ;
SELECT COUNT(1) FROM store_sales           ;
SELECT COUNT(1) FROM time_dim              ;
SELECT COUNT(1) FROM warehouse             ;
SELECT COUNT(1) FROM web_page              ;
SELECT COUNT(1) FROM web_returns           ;
SELECT COUNT(1) FROM web_sales             ;
SELECT COUNT(1) FROM web_site              ;

```

9.新建简易limit查询SQL，主要验证内部表数据是否能够被正常查询
vim /root/tpc_ds_test/sql/kudu/kudu_limit_query.sql
将下面👇的SQL复制进去

```
use tpc_test;

SELECT * FROM call_center           limit 2;
SELECT * FROM catalog_page          limit 2;
SELECT * FROM catalog_returns       limit 2;
SELECT * FROM catalog_sales         limit 2;
SELECT * FROM customer              limit 2;
SELECT * FROM customer_address      limit 2;
SELECT * FROM customer_demographics limit 2;
SELECT * FROM date_dim              limit 2;
SELECT * FROM dbgen_version         limit 2;
SELECT * FROM household_demographics limit 2;
SELECT * FROM income_band           limit 2;
SELECT * FROM inventory             limit 2;
SELECT * FROM item                  limit 2;
SELECT * FROM promotion             limit 2;
SELECT * FROM reason                limit 2;
SELECT * FROM ship_mode             limit 2;
SELECT * FROM store                 limit 2;
SELECT * FROM store_returns         limit 2;
SELECT * FROM store_sales           limit 2;
SELECT * FROM time_dim              limit 2;
SELECT * FROM warehouse             limit 2;
SELECT * FROM web_page              limit 2;
SELECT * FROM web_returns           limit 2;
SELECT * FROM web_sales             limit 2;
SELECT * FROM web_site              limit 2;

```


10.准备start_tpc_import.sh导数脚本。
start_tpc_import.sh在初始化目录时已经创建完成
cd /root/tpc_ds_test/bin && vim start_tpc_import.sh

脚本执行内容：
1.新建impala外部表ddl。
2.执行impala外部表COUNT和limit查询。
3.执行kudu内部表ddl。
4.将impala外部表数据导入到kudu内部表。
5.统计分析kudu的内部表，执行compute stats table_name。
5.执行kudu内部表COUNT和limit查询。
6.收集执行结果信息和耗时，tpc_ds_test/result目录查看结果，tpc_ds_test/logs目录查看执行过程日志。


将下面👇的脚本复制进去

```
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
        if [[ "$condition" != "" ]] && [[ "$condition" == *"SELECT"* ]];
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

```

11.执行start_tpc_import.sh
cd /root/tpc_ds_test/bin && sh start_tpc_import.sh

执行结果查看：/root/tpc_ds_test/result
```
ext_count_result.csv
    1.25张外部表（impala外部表）
    2.对所有外部表做COUNT操作的详细结果

import_test_data_result.csv
    1.将25张外部表导入到25张kudu内部表的详细结果

kudu_count_result.csv
    1.25张内部表（kudu内部表）
    2.对所有内部表做COUNT操作的详细结果
    
kudu_analyze_result
    1.对25张kudu表进行 compute status table_name 结果和耗时

```

        
执行日志查看：/root/tpc_ds_test/logs
```
[root@test4 logs]# pwd
/root/tpc_ds_test/logs
[root@test4 logs]# ll
total 4
drwxr-xr-x 2 root root    6 Jun  9 19:30 ext
drwxr-xr-x 2 root root    6 Jun  9 19:30 import
drwxr-xr-x 2 root root   62 Jun  9 19:33 kudu
drwxr-xr-x 2 root root 4096 Jun  9 11:02 query
```

## 数据导入成功进行TPC—DS提供的基准查询

前面我们使用了dsqgen工具生成了基准查询SQL语句，SQL文件：/root/tpc_ds_test/query_data/query_0.sql
打开后发现，有很多SQL语法与kudu不兼容。经过手动改造，整理出来61个可用的，其他的暂时先注释。
将SQL内容重新给复制到query_0.sql文件中。

参考👇：

```
内容过多，参考附件 /root/tpc_ds_test/query_data/query_0.sql
```

1.准备start_kudu_query.sh执行基准查询脚本。
start_kudu_query.sh在初始化目录时已经创建完成
cd /root/tpc_ds_test/bin && vim start_kudu_query.sh

脚本执行内容：
1.分割query_0.sql的每个SQL。进行单独的查询。
2.收集每个SQL的执行结果和耗时信息。tpc_ds_test/result目录查看结果，tpc_ds_test/logs目录查看执行过程日志。

将下面👇的脚本复制进去
```
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

```


2.执行start_kudu_query.sh
cd /root/tpc_ds_test/bin && sh start_kudu_query.sh

执行结果查看：/root/tpc_ds_test/result
```
query_result.csv
    1.TPC-DS自带的61个复杂的SQL查询。
    2.每个SQL的执行详细信息以及查询的结果和耗时。
```


参考链接：
[TPC-DS测试](https://blog.csdn.net/zyl651334919/article/details/88837004)
[TPC-DS在大数据中的使用
](https://www.jianshu.com/p/173de219379e#5)[impala 建表，外部文件数据导入impala中_数据库](https://blog.csdn.net/liuxiangke0210/article/details/50439844)
[TPC-DS性能测试及使用方法_大数据_u011563666的博客-CSDN博客](https://blog.csdn.net/u011563666/article/details/78751584)
[如何进行TPS-DS测试_数据库_huangmr的专栏-CSDN博客](https://blog.csdn.net/huangjin0507/article/details/54426686)