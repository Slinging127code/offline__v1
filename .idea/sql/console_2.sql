use xinyi_jiao;

DROP TABLE IF EXISTS ods_activity_info;
CREATE EXTERNAL TABLE ods_activity_info(
                                           `id` STRING COMMENT '编号',
                                           `activity_name` STRING COMMENT '活动名称',
                                           `activity_type` STRING COMMENT '活动类型',
                                           `start_time` STRING COMMENT '开始时间',
                                           `end_time` STRING COMMENT '结束时间',
                                           `create_time` STRING COMMENT '创建时间'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_activity_info'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

select * from ods_activity_info;
load data inpath '/2207A/xinyi_jiao/activity_info/2025-03-24' into table ods_activity_info partition (dt='2025-03-24');


DROP TABLE IF EXISTS ods_activity_order;
CREATE EXTERNAL TABLE ods_activity_order(
                                           `id` int COMMENT '编号',
                                           `activity_id` int COMMENT '活动id',
                                           `order_id` int COMMENT '订单编号',
                                           `create_time` STRING COMMENT '发生日期'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_activity_order'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

select * from ods_activity_order;
load data inpath '/2207A/xinyi_jiao/activity_order/2025-03-24' into table ods_activity_order partition (dt='2025-03-24');

----

DROP TABLE IF EXISTS ods_activity_rule;
CREATE EXTERNAL TABLE ods_activity_rule(
                                           `id` STRING COMMENT '编号',
                                           `activity_id` STRING COMMENT '活动 ID',
                                           `activity_type` STRING COMMENT '活动类型',
                                           `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
                                           `condition_num` BIGINT COMMENT '满减件数',
                                           `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
                                           `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
                                           `benefit_level` STRING COMMENT '优惠级别'
) COMMENT '活动规则表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_activity_rule'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );


load data inpath '/2207A/xinyi_jiao/activity_rule/2025-03-24' into table ods_activity_rule partition (dt='2025-03-24');
select * from ods_activity_rule;
------
DROP TABLE IF EXISTS ods_base_category1;
CREATE EXTERNAL TABLE ods_base_category1(
                                            `id` STRING COMMENT 'id',
                                            `name` STRING COMMENT '名称'
) COMMENT '商品一级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_base_category1'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );

load data inpath '/2207A/xinyi_jiao/base_category1/2025-03-24' into table ods_base_category1 partition (dt='2025-03-24');
select * from ods_base_category1;

-----
DROP TABLE IF EXISTS ods_base_category2;
CREATE EXTERNAL TABLE ods_base_category2(
                                            `id` STRING COMMENT ' id',
                                            `name` STRING COMMENT '名称',
                                            `category1_id` STRING COMMENT '一级品类 id'
) COMMENT '商品二级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_base_category2/'
    TBLPROPERTIES (
        'hive.exec.compress.output' = 'true',
        'mapreduce.output.fileoutputformat.compress' = 'true',
        'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
        );
load data inpath '/2207A/xinyi_jiao/base_category2/2025-03-24' into table ods_base_category2 partition (dt='2025-03-24');
select * from ods_base_category2;

------
DROP TABLE IF EXISTS ods_base_category3;
CREATE EXTERNAL TABLE ods_base_category3(
                                            `id` STRING COMMENT ' id',
                                            `name` STRING COMMENT '名称',
                                            `category2_id` STRING COMMENT '二级品类 id'
) COMMENT '商品三级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_base_category3/' TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/base_category3/2025-03-24' into table ods_base_category3 partition (dt='2025-03-24');
select * from ods_base_category3;
---
DROP TABLE IF EXISTS ods_base_province;
CREATE EXTERNAL TABLE ods_base_province (
                                            `id` STRING COMMENT '编号',
                                            `name` STRING COMMENT '省份名称',
                                            `region_id` STRING COMMENT '地区 ID',
                                            `area_code` STRING COMMENT '地区编码',
                                            `iso_code` STRING COMMENT 'ISO-3166 编码，供可视化使用',
                                            `iso_3166_2` STRING COMMENT 'IOS-3166-2 编码，供可视化使用'
) COMMENT '省份表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_base_province/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/base_province/2025-03-24' into table ods_base_province;
select * from ods_base_province;

---
DROP TABLE IF EXISTS ods_base_region;
CREATE EXTERNAL TABLE ods_base_region (
                                          `id` STRING COMMENT '编号',
                                          `region_name` STRING COMMENT '地区名称'
) COMMENT '地区表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_base_region/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/base_region/2025-03-24' into table ods_base_region;
select * from ods_base_region;
----
DROP TABLE IF EXISTS ods_base_trademark;
CREATE EXTERNAL TABLE ods_base_trademark (
                                             `id` STRING COMMENT '编号',
                                             `tm_name` STRING COMMENT '品牌名称'
) COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_base_trademark/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/base_trademark/2025-03-24' into table ods_base_trademark partition (dt='2025-03-24');;
select * from ods_base_trademark;

--
DROP TABLE IF EXISTS ods_cart_info;
CREATE EXTERNAL TABLE ods_cart_info(
                                       `id` STRING COMMENT '编号',
                                       `user_id` STRING COMMENT '用户 id',
                                       `sku_id` STRING COMMENT 'skuid',
                                       `cart_price` DECIMAL(16,2) COMMENT '放入购物车时价格',
                                       `sku_num` BIGINT COMMENT '数量',
                                       `sku_name` STRING COMMENT 'sku 名称 (冗余)',
                                       `create_time` STRING COMMENT '创建时间',
                                       `operate_time` STRING COMMENT '修改时间',
                                       `is_ordered` STRING COMMENT '是否已经下单',
                                       `order_time` STRING COMMENT '下单时间',
                                       `source_type` STRING COMMENT '来源类型',
                                       `source_id` STRING COMMENT '来源编号'
) COMMENT '加购表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_cart_info/' TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/cart_info/2025-03-24' into table ods_cart_info partition (dt='2025-03-24');;
select * from ods_cart_info;

---
DROP TABLE IF EXISTS ods_comment_info;
CREATE EXTERNAL TABLE ods_comment_info(
                                          `id` STRING COMMENT '编号',
                                          `user_id` STRING COMMENT '用户 ID',
                                          `sku_id` STRING COMMENT '商品 sku',
                                          `spu_id` STRING COMMENT '商品 spu',
                                          `order_id` STRING COMMENT '订单 ID',
                                          `appraise` STRING COMMENT '评价',
                                          `create_time` STRING COMMENT '评价时间'
) COMMENT '商品评论表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_comment_info/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/comment_info/2025-03-24' into table ods_comment_info partition (dt='2025-03-24');;
select * from ods_comment_info;

---
DROP TABLE IF EXISTS ods_coupon_info;
CREATE EXTERNAL TABLE ods_coupon_info(
                                         `id` STRING COMMENT '购物券编号',
                                         `coupon_name` STRING COMMENT '购物券名称',
                                         `coupon_type` STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减
券 4 满件打折券',
                                         `condition_amount` DECIMAL(16,2) COMMENT '满额数',
                                         `condition_num` BIGINT COMMENT '满件数',
                                         `activity_id` STRING COMMENT '活动编号',
                                         `benefit_amount` DECIMAL(16,2) COMMENT '减金额',
                                         `benefit_discount` DECIMAL(16,2) COMMENT '折扣',
                                         `create_time` STRING COMMENT '创建时间',
                                         `range_type` STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
                                         `limit_num` BIGINT COMMENT '最多领用次数',
                                         `taken_count` BIGINT COMMENT '已领用次数',
                                         `start_time` STRING COMMENT '开始领取时间',
                                         `end_time` STRING COMMENT '结束领取时间',
                                         `operate_time` STRING COMMENT '修改时间',
                                         `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_coupon_info/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/coupon_info/2025-03-24' into table ods_coupon_info partition (dt='2025-03-24');;
select * from ods_coupon_info;
--

DROP TABLE IF EXISTS ods_coupon_use;
CREATE EXTERNAL TABLE ods_coupon_use(
                                        `id` STRING COMMENT '编号',
                                        `coupon_id` STRING COMMENT '优惠券 ID',
                                        `user_id` STRING COMMENT 'skuid',
                                        `order_id` STRING COMMENT 'spuid',
                                        `coupon_status` STRING COMMENT '优惠券状态',
                                        `get_time` STRING COMMENT '领取时间',
                                        `using_time` STRING COMMENT '使用时间(下单)',
                                        `used_time` STRING COMMENT '使用时间(支付)',
                                        `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_coupon_use/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/coupon_use/2025-03-24' into table ods_coupon_use partition (dt='2025-03-24');;
select * from ods_coupon_use;
---
DROP TABLE IF EXISTS ods_favor_info;
CREATE EXTERNAL TABLE ods_favor_info(
                                        `id` STRING COMMENT '编号',
                                        `user_id` STRING COMMENT '用户 id',
                                        `sku_id` STRING COMMENT 'skuid',
                                        `spu_id` STRING COMMENT 'spuid',
                                        `is_cancel` STRING COMMENT '是否取消',
                                        `create_time` STRING COMMENT '收藏时间',
                                        `cancel_time` STRING COMMENT '取消时间'
) COMMENT '商品收藏表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_favor_info/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/favor_info/2025-03-24' into table ods_favor_info partition (dt='2025-03-24');;
select * from ods_favor_info;
--
DROP TABLE IF EXISTS ods_order_detail;
CREATE EXTERNAL TABLE ods_order_detail(
                                          `id` STRING COMMENT '编号',
                                          `order_id` STRING COMMENT '订单号',
                                          `sku_id` STRING COMMENT '商品 id',
                                          `sku_name` STRING COMMENT '商品名称',
                                          `order_price` DECIMAL(16,2) COMMENT '商品价格',
                                          `sku_num` BIGINT COMMENT '商品数量',
                                          `create_time` STRING COMMENT '创建时间',
                                          `source_type` STRING COMMENT '来源类型',
                                          `source_id` STRING COMMENT '来源编号',
                                          `split_final_amount` DECIMAL(16,2) COMMENT '分摊最终金额',
                                          `split_activity_amount` DECIMAL(16,2) COMMENT '分摊活动优惠',
                                          `split_coupon_amount` DECIMAL(16,2) COMMENT '分摊优惠券优惠'
) COMMENT '订单详情表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_order_detail/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/order_detail/2025-03-24' into table ods_order_detail partition (dt='2025-03-24');;
select * from ods_order_detail;
---
DROP TABLE IF EXISTS ods_order_info;
CREATE EXTERNAL TABLE ods_order_info (
                                         `id` STRING COMMENT '订单号',
                                         `final_amount` DECIMAL(16,2) COMMENT '订单最终金额',
                                         `order_status` STRING COMMENT '订单状态',
                                         `user_id` STRING COMMENT '用户 id',
                                         `payment_way` STRING COMMENT '支付方式',
                                         `delivery_address` STRING COMMENT '送货地址',
                                         `out_trade_no` STRING COMMENT '支付流水号',
                                         `create_time` STRING COMMENT '创建时间',
                                         `operate_time` STRING COMMENT '操作时间',
                                         `expire_time` STRING COMMENT '过期时间',
                                         `tracking_no` STRING COMMENT '物流单编号',
                                         `province_id` STRING COMMENT '省份 ID',
                                         `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免金额',
                                         `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免金额',
                                         `original_amount` DECIMAL(16,2) COMMENT '订单原价金额',
                                         `feight_fee` DECIMAL(16,2) COMMENT '运费',
                                         `feight_fee_reduce` DECIMAL(16,2) COMMENT '运费减免'
) COMMENT '订单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_order_info/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/order_info/2025-03-24' into table ods_order_info partition (dt='2025-03-24');;
select * from ods_order_info;
--
DROP TABLE IF EXISTS ods_order_refund_info;
CREATE EXTERNAL TABLE ods_order_refund_info(
                                               `id` STRING COMMENT '编号',
                                               `user_id` STRING COMMENT '用户 ID',
                                               `order_id` STRING COMMENT '订单 ID',
                                               `sku_id` STRING COMMENT '商品 ID',
                                               `refund_type` STRING COMMENT '退单类型',
                                               `refund_num` BIGINT COMMENT '退单件数',
                                               `refund_amount` DECIMAL(16,2) COMMENT '退单金额',
                                               `refund_reason_type` STRING COMMENT '退单原因类型',
                                               `refund_status` STRING COMMENT '退单状态',--退单状态应包含买家申 请、卖家审核、卖家收货、退款完成等状态。此处未涉及到，故该表按增量处理
                                               `create_time` STRING COMMENT '退单时间'
) COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/2207A/xinyi_jiao/ods/ods_order_refund_info/'TBLPROPERTIES (
    'hive.exec.compress.output' = 'true',
    'mapreduce.output.fileoutputformat.compress' = 'true',
    'mapreduce.output.fileoutputformat.compress.codec' = 'org.apache.hadoop.io.compress.GzipCodec'
    );
load data inpath '/2207A/xinyi_jiao/order_refund_info/2025-3-24' into table ods_order_info partition (dt='2025-03-24');
select * from ods_order_refund_info;