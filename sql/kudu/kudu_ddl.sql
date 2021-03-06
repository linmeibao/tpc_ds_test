
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
