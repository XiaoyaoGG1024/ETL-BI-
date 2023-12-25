#! /bin/bash

hive -e"
CREATE TABLE  IF NOT EXISTS dwd.dim_customer_inf (
  customer_inf_id int     COMMENT '自增主键ID',
  customer_id int    COMMENT 'customer_login表的自增ID',
  customer_name varchar(20)   COMMENT '用户真实姓名',
  identity_card_type tinyint    COMMENT '证件类型：1 身份证，2 军官证，3 护照',
  identity_card_no varchar(20)   COMMENT '证件号码',
  mobile_phone varchar(50)   COMMENT '手机号',
  customer_email varchar(50)   COMMENT '邮箱',
  gender char(1)   COMMENT '性别',
  customer_point int   COMMENT '用户积分',
  register_time timestamp   COMMENT'注册时间',
  birthday timestamp   COMMENT '会员生日',
  customer_level tinyint   COMMENT '会员级别：1 普通会员，2 青铜，3白银，4黄金，5钻石',
  customer_money decimal(8,2)    COMMENT '用户余额',
  modified_time timestamp   COMMENT '最后修改时间'
)
partitioned by (elt_date string);
CREATE TABLE IF NOT EXISTS dwd.fact_order_detail (
  order_detail_id int  COMMENT '订单详情表ID',
  order_sn varchar(100)  COMMENT '订单编号',
  product_id int   COMMENT '订单商品ID',
  product_name varchar(50)  COMMENT '商品名称',
  product_cnt int    COMMENT '购买商品数量',
  product_price decimal(8,2)  COMMENT '购买商品单价',
  average_cost decimal(8,2)  COMMENT '平均成本价格',
  weight float  COMMENT '商品重量',
  fee_money decimal(8,2)   COMMENT '优惠分摊金额',
  w_id int   COMMENT '仓库ID',
  create_time varchar(200)  COMMENT '创建时间',
  modified_time timestamp COMMENT '最后修改时间'
)
partitioned by (elt_date string);
CREATE TABLE IF NOT EXISTS dwd.fact_order_master (
  order_id  int COMMENT '订单ID',
  order_sn varchar(100)  COMMENT '订单编号',
  customer_id  int COMMENT '下单人ID',
  shipping_user varchar(10)  COMMENT '收货人姓名',
  province varchar(200)  COMMENT '省',
  city varchar(200)  COMMENT '市',
  address varchar(100)  COMMENT '地址',
  order_source tinyint  COMMENT '订单来源：1直接浏览，2购物车',
  payment_method tinyint  COMMENT '支付方式：1现金，2余额，3网银，4支付宝，5微信',
  order_money decimal(8,2)  COMMENT '订单金额',
  district_money decimal(8,2)  COMMENT '优惠金额',
  shipping_money decimal(8,2)  COMMENT '运费金额',
  payment_money decimal(8,2)  COMMENT '支付金额',
  shipping_comp_name varchar(10)  COMMENT '快递公司名称',
  shipping_sn varchar(50)  COMMENT '快递单号',
  create_time varchar(100)  COMMENT '下单时间',
  shipping_time varchar(100)  COMMENT '发货时间',
  pay_time varchar(100)  COMMENT '支付时间',
  receive_time varchar(100)  COMMENT '收货时间',
  order_status varchar(50)  COMMENT '快递状态',
  order_point  int  COMMENT '订单积分',
  invoice_title varchar(100)  COMMENT '发票抬头',
  modified_time timestamp   COMMENT '最后修改时间'
)partitioned by (elt_date string);
CREATE TABLE IF NOT EXISTS  dwd.dim_product_info (
  product_id  int COMMENT '商品ID',
  product_core char(16)  COMMENT '商品编码',
  product_name varchar(200)  COMMENT '商品名称',
  bar_code varchar(50)  COMMENT '国条码',
  brand_id  int COMMENT '品牌表的ID',
  one_category_id smallint  COMMENT '一级分类ID',
  two_category_id smallint  COMMENT '二级分类ID',
  three_category_id smallint COMMENT '三级分类ID',
  supplier_id  int COMMENT '商品的供应商ID',
  price decimal(8,2)  COMMENT '商品销售价格',
  average_cost decimal(18,2)  COMMENT '商品加权平均成本',
  publish_status tinyint   COMMENT '上下架状态：0下架1上架',
  audit_status tinyint   COMMENT '审核状态：0未审核，1已审核',
  weight float  COMMENT '商品重量',
  length float  COMMENT '商品长度',
  height float  COMMENT '商品高度',
  width float  COMMENT '商品宽度',
  color_type string ,
  production_date timestamp  COMMENT '生产日期',
  shelf_life int  COMMENT '商品有效期',
  descript string  COMMENT '商品描述',
  indate timestamp  COMMENT '商品录入时间',
  modified_time timestamp  COMMENT '最后修改时间'
) partitioned by (elt_date string);

alter table dwd.dim_customer_inf add columns(dwd_insert_user string,dwd_insert_time string,dwd_modify_user string,dwd_modify_time string)cascade;
alter table dwd.dim_product_info add columns(dwd_insert_user string,dwd_insert_time string,dwd_modify_user string,dwd_modify_time string)cascade;
alter table dwd.fact_order_master add columns(dwd_insert_user string,dwd_insert_time string,dwd_modify_user string,dwd_modify_time string)cascade;
alter table dwd.fact_order_detail add columns(dwd_insert_user string,dwd_insert_time string,dwd_modify_user string,dwd_modify_time string)cascade;
"
