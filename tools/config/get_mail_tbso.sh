#########################################################################
# Author: lilong(longlee08@gmail.com)
# Created Time: Fri 12 Apr 2013 04:02:47 PM CST
# File Name: get_data.sh
# Description: 
#########################################################################
#!/bin/bash

source /home/tbso/conf/set_env.sh
source /home/tbso/conf/common_tables.sh
source /home/tbso/conf/common_utils.sh 


DATE_YESTERDAY=`date +%Y%m%d -d -1day`
DATE_LOG_DEL=`date +%Y%m%d -d -10day`
WORKDIR=/home/lilong.ll/model_auction_fake

function get_tbso_mail()
{
    $HADOOP fs -cat /group/tbso/hive/${1}/* |tr '' ','|iconv -f utf-8 -t gbk >${1}.csv
    
    if [ $# -lt 2 ]
    then
        echo "data:${1}"
        exit
    fi
    
    NUM=`wc -l ${1}.csv`

    /home/lilong.ll/bin/sendEmail \
    -f "lilong.ll@taobao.com" \
    -u "data:${1}" \
    -m "date:${DATE_YESTERDAY},num:${NUM}" \
    -t "${2}" \
    -cc "lilong.ll@taobao.com" \
    -a "./${1}.csv"

}

get_tbso_mail t_ll_fake_cellphone_3c_result shuyun.xcm@taobao.com
