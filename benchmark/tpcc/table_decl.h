// -*- mode: c++ -*-

#ifndef TABLE_DECL_H
#define TABLE_DECL_H

#include "sqltypes.h"

namespace sql {

FIELD(uint32_t , c_w_id) ;
FIELD(uint32_t , c_d_id) ;
FIELD(uint32_t , c_id)   ;
KEYS(CustomerKey);

FIELD(int                       , c_discount)     ;
FIELD(sql::Char<2>  , c_credit)       ;
FIELD(sql::inline_str_8<16>     , c_last)         ;
FIELD(sql::inline_str_8<16>     , c_first)        ;
FIELD(int                       , c_credit_lim)   ;
FIELD(int                       , c_balance)      ;
FIELD(int                       , c_ytd_payment)  ;
FIELD(int32_t                   , c_payment_cnt)  ;
FIELD(int32_t                   , c_delivery_cnt) ;
FIELD(sql::inline_str_8<20>     , c_street_1)     ;
FIELD(sql::inline_str_8<20>     , c_street_2)     ;
FIELD(sql::inline_str_8<20>     , c_city)         ;
FIELD(sql::Char<2>  , c_state)        ;
FIELD(sql::Char<9>  , c_zip)          ;
FIELD(sql::Char<16> , c_phone)        ;
FIELD(uint32_t                  , c_since)        ;
FIELD(sql::Char<2>  , c_middle)       ;
FIELD(sql::inline_str_16<500>   , c_data)         ;
VALUES(CustomerValue);

FIELD(uint32_t,  c_w_id) ;
FIELD(uint32_t,  c_d_id) ;
FIELD(sql::Char<16>, c_last);
FIELD(sql::Char<16>, c_first);
KEYS(CustomerNameIdxKey);

FIELD(uint32_t,  c_id) ;
VALUES(CustomerNameIdxValue);

FIELD(uint32_t, d_w_id) ;
FIELD(uint32_t, d_id)   ;
KEYS(DistrictKey);

FIELD(int32_t                  , d_ytd)       ;
FIELD(int32_t                  , d_tax)       ;
FIELD(uint32_t                 , d_next_o_id) ;
FIELD(sql::inline_str_8<10>    , d_name)      ;
FIELD(sql::inline_str_8<20>    , d_street_1)  ;
FIELD(sql::inline_str_8<20>    , d_street_2)  ;
FIELD(sql::inline_str_8<20>    , d_city)      ;
FIELD(sql::Char<2> , d_state)     ;
FIELD(sql::Char<9> , d_zip)       ;
VALUES(DistrictValue);

FIELD(uint32_t , h_c_id  ) ;
FIELD(uint32_t , h_c_d_id) ;
FIELD(uint32_t , h_c_w_id) ;
FIELD(uint32_t , h_d_id  ) ;
FIELD(uint32_t , h_w_id  ) ;
FIELD(uint32_t , h_date  ) ;
KEYS(HistoryKey);


FIELD(int32_t               , h_amount) ;
FIELD(sql::inline_str_8<24> , h_data)   ;
VALUES(HistoryValue);

FIELD(uint32_t , i_id) ;
KEYS(ItemKey);

FIELD(sql::inline_str_8<24> , i_name)  ;
FIELD(int32_t               , i_price) ;
FIELD(sql::inline_str_8<50> , i_data)  ;
FIELD(int32_t               , i_im_id) ;
VALUES(ItemValue);

FIELD(uint32_t , no_w_id);
FIELD(uint32_t , no_d_id);
FIELD(uint32_t , no_o_id);
KEYS(NewOrderKey);

FIELD(sql::Char<12> , no_dummy);
VALUES(NewOrderValue);

FIELD(uint32_t                  , o_w_id) ;
FIELD(uint32_t                  , o_d_id) ;
FIELD(uint32_t                  , o_id)   ;
KEYS(OOrderKey);

FIELD(uint32_t                  , o_c_id)       ;
FIELD(uint32_t                  , o_carrier_id) ;
FIELD(uint8_t                   , o_ol_cnt)     ;
FIELD(bool                      , o_all_local)  ;
FIELD(uint32_t                  , o_entry_d)    ;
VALUES(OOrderValue);

FIELD(uint32_t                  , o_w_id) ;
FIELD(uint32_t                  , o_d_id) ;
FIELD(uint32_t                  , o_c_id) ;
FIELD(uint32_t                  , o_o_id) ;
KEYS(OOrderCIdIdxKey);

FIELD(uint8_t                   , o_dummy) ;
VALUES(OOrderCIdIdxValue);

FIELD(uint32_t                  , ol_w_id) ;
FIELD(uint32_t                  , ol_d_id) ;
FIELD(uint32_t                  , ol_o_id) ;
FIELD(uint32_t                  , ol_number);
KEYS(OrderLineKey);

FIELD(int32_t                   , ol_i_id)        ;
FIELD(uint32_t                  , ol_delivery_d)  ;
FIELD(int32_t                   , ol_amount)      ;
FIELD(int32_t                   , ol_supply_w_id) ;
FIELD(uint8_t                   , ol_quantity)    ;
VALUES(OrderLineValue);

FIELD(uint32_t                  , s_w_id) ;
FIELD(uint32_t                  , s_i_id) ;
KEYS(StockKey);

FIELD(int16_t                   , s_quantity)   ;
FIELD(int32_t                   , s_ytd)        ;
FIELD(int32_t                   , s_order_cnt)  ;
FIELD(int32_t                   , s_remote_cnt) ;
VALUES(StockValue);

FIELD(uint32_t                  , s_w_id) ;
FIELD(uint32_t                  , s_i_id) ;
KEYS(StockDataKey);

FIELD(sql::inline_str_8<50>     , s_data)    ;
FIELD(sql::Char<24> , s_dist_01) ;
FIELD(sql::Char<24> , s_dist_02) ;
FIELD(sql::Char<24> , s_dist_03) ;
FIELD(sql::Char<24> , s_dist_04) ;
FIELD(sql::Char<24> , s_dist_05) ;
FIELD(sql::Char<24> , s_dist_06) ;
FIELD(sql::Char<24> , s_dist_07) ;
FIELD(sql::Char<24> , s_dist_08) ;
FIELD(sql::Char<24> , s_dist_09) ;
FIELD(sql::Char<24> , s_dist_10) ;
VALUES(StockDataValue);

FIELD(uint32_t                  , w_id) ;
KEYS(WarehouseKey);

FIELD(int32_t                   , w_ytd)      ;
FIELD(int32_t                   , w_tax)      ;
FIELD(sql::inline_str_8<10>     , w_name)     ;
FIELD(sql::inline_str_8<20>     , w_street_1) ;
FIELD(sql::inline_str_8<20>     , w_street_2) ;
FIELD(sql::inline_str_8<20>     , w_city)     ;
FIELD(sql::Char<2>  , w_state)    ;
FIELD(sql::Char<9>  , w_zip)      ;
VALUES(WarehouseValue);

}

#endif /* TABLE_DECL_H */
