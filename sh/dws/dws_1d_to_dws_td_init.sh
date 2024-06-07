#! /bin/bash
#1、判断参数是否传入
if [ $# -lt 2 ]
then
	echo "必须传入all/表名与数仓上线日期..."
	exit
fi

dws_trans_dispatch_td_sql="
insert overwrite table dws_trans_dispatch_td
    partition (dt = '$2')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d;
"

dws_trans_bound_finish_td_sql="
insert overwrite table dws_trans_bound_finish_td
    partition (dt = '$2')
select count(order_id)   order_count,
       sum(order_amount) order_amount
from (select order_id,
             max(amount) order_amount
      from dwd_trans_bound_finish_detail_inc
      group by order_id) distinct_info;
"
#2、匹配表名加载数据
case $1 in
"all")
	/opt/module/hive/bin/hive -e "use tms;${dws_trans_bound_finish_td_sql}${dws_trans_dispatch_td_sql}"
;;
"dws_trans_bound_finish_td")
    /opt/module/hive/bin/hive -e "use tms;${dws_trans_bound_finish_td_sql}"
;;
"dws_trans_dispatch_td")
    /opt/module/hive/bin/hive -e "use tms;${dws_trans_dispatch_td_sql}"
;;
*)
	echo "表名输入错误..."
;;
esac