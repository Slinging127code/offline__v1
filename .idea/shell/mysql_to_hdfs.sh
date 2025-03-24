#!/bin/bash

DATAX_HOME=/opt/soft/datax

# 如果传入日期则do_date等于传入的日期，否则等于前一天日期

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

#处理目标路径，此处的处理逻辑是，如果目标路径不存在，则创建；若存在，则清空，目的是保证同步任务可重复执行
handle_targetdir() {
  hdfs dfs -test -e $1
  if [[ $? -eq 1 ]]; then
    echo "路径$1不存在，正在创建......"
    hdfs dfs -mkdir -p $1
  else
    echo "路径$1已经存在"
    fs_count=$(hdfs dfs -count $1)
    content_size=$(echo $fs_count | awk '{print $3}')
    if [[ $content_size -eq 0 ]]; then
      echo "路径$1为空"
    else
      echo "路径$1不为空，正在清空......"
      hdfs dfs -rm -r -f $1/*
    fi
  fi
}

#数据同步
import_data() {
  datax_config=$1
  target_dir=$2
  handle_targetdir $target_dir
  python $DATAX_HOME/bin/datax.py -p" -Dtargetdir=$target_dir" $datax_config
}

case $1 in
"order_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.order_info.json /2207A/xinyi_jiao/order_info/$do_date
  ;;
"base_category1")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_category1.json /2207A/xinyi_jiao/base_category1/$do_date
  ;;
"base_category2")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_category2.json /2207A/xinyi_jiao/base_category2/$do_date
  ;;
"base_category3")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_category3.json /2207A/xinyi_jiao/base_category3/$do_date
  ;;
"order_detail")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.order_detail.json /2207A/xinyi_jiao/order_detail/$do_date
  ;;
"sku_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.sku_info.json /2207A/xinyi_jiao/sku_info/$do_date
  ;;
"user_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.user_info.json /2207A/xinyi_jiao/user_info/$do_date
  ;;
"payment_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.payment_info.json /2207A/xinyi_jiao/payment_info/$do_date
  ;;
"base_province")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_province.json /2207A/xinyi_jiao/base_province/$do_date
  ;;
"base_region")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_region.json /2207A/xinyi_jiao/base_region/$do_date
  ;;
"base_trademark")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_trademark.json /2207A/xinyi_jiao/base_trademark/$do_date
  ;;
"activity_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.activity_info.json /2207A/xinyi_jiao/activity_info/$do_date
  ;;
"activity_order")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.activity_order.json /2207A/xinyi_jiao/activity_order/$do_date
  ;;
"cart_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.cart_info.json /2207A/xinyi_jiao/cart_info/$do_date
  ;;
"comment_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.comment_info.json /2207A/xinyi_jiao/comment_info/$do_date
  ;;
"coupon_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.coupon_info.json /2207A/xinyi_jiao/coupon_info/$do_date
  ;;
"coupon_use")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.coupon_use.json /2207A/xinyi_jiao/coupon_use/$do_date
  ;;
"favor_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.favor_info.json /2207A/xinyi_jiao/favor_info/$do_date
  ;;
"order_refund_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.order_refund_info.json /2207A/xinyi_jiao/order_refund_info/$do_date
  ;;
"order_status_log")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.order_status_log.json /2207A/xinyi_jiao/order_status_log/$do_date
  ;;
"spu_info")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.spu_info.json /2207A/xinyi_jiao/spu_info/$do_date
  ;;
"activity_rule")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.activity_rule.json /2207A/xinyi_jiao/activity_rule/$do_date
  ;;
"base_dic")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_dic.json /2207A/xinyi_jiao/base_dic/$do_date
  ;;
"all")
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.order_info.json /2207A/xinyi_jiao/order_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_category1.json /2207A/xinyi_jiao/base_category1/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_category2.json /2207A/xinyi_jiao/base_category2/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_category3.json /2207A/xinyi_jiao/base_category3/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.order_detail.json /2207A/xinyi_jiao/order_detail/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.sku_info.json /2207A/xinyi_jiao/sku_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.user_info.json /2207A/xinyi_jiao/user_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.payment_info.json /2207A/xinyi_jiao/payment_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_province.json /2207A/xinyi_jiao/base_province/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_region.json /2207A/xinyi_jiao/base_region/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_trademark.json /2207A/xinyi_jiao/base_trademark/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.activity_info.json /2207A/xinyi_jiao/activity_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.activity_order.json /2207A/xinyi_jiao/activity_order/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.cart_info.json /2207A/xinyi_jiao/cart_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.comment_info.json /2207A/xinyi_jiao/comment_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.coupon_info.json /2207A/xinyi_jiao/coupon_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.coupon_use.json /2207A/xinyi_jiao/coupon_use/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.favor_info.json /2207A/xinyi_jiao/favor_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.order_refund_info.json /2207A/xinyi_jiao/order_refund_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.order_status_log.json /2207A/xinyi_jiao/order_status_log/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.spu_info.json /2207A/xinyi_jiao/spu_info/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.activity_rule.json /2207A/xinyi_jiao/activity_rule/$do_date
  import_data /opt/soft/datax/job/2207A/xinyi_jiao/import/dev_realtime_v1_xinyu_luo.base_dic.json /2207A/xinyi_jiao/base_dic/$do_date
  ;;
esac