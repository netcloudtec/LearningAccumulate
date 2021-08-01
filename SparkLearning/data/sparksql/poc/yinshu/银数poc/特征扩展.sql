-- 同卡前几天atm交易次数
-- 同卡前几天atm交易金额
-- 同卡前几天交易终端数      finish
-- 同卡前几天交易商户数      finish
-- 同卡前几天交易商户类型数  finish
-- 同商户前几天交易银行的卡数 finish
-- 同商户前几天正交易的银行卡数
-- 同商户前几天正交易的次数
-- 同商户前几天正交易金额
-- 同卡同商户前几天正交易的次数
-- 同卡同商户前几天正交易的金额

-- 同卡前几天总金额                     exists
-- 同卡前几天总次数                     exists
-- 同卡前几天总最大金额                    ？
-- 同卡前几天总最小金额                    ？
-- 同卡前几天交易金额为负的最大金额      
-- 同卡前几天交易金额为正的最小金额  
-- 同卡前几天还款次数
-- 同卡前几天余额查询次数
-- 同卡前几天交易城市数
-- 同卡前几天交易国家数
-- 同商户前几天总次数
-- 同商户前几天总金额
-- 同商户前几天总最大金额
-- 同商户前几天总最小金额
-- 同商户前几天交易金额为负的最大金额
-- 同商户前几天交易金额为正的最小金额


交易终端编号


count(sd_term_id) over (partition by sd_pan order by local_unix_timestamp range between 3600 preceding and 1 preceding) sd_term_id_1hour_count,
count(sd_term_id) over (partition by sd_pan order by local_unix_timestamp range between 86400 preceding and 1 preceding) sd_term_id_1day_count,
count(sd_term_id) over (partition by sd_pan order by local_unix_timestamp range between 604800 preceding and 1 preceding) sd_term_id_7day_count,
count(sd_term_id) over (partition by sd_pan order by local_unix_timestamp range between 1296000 preceding and 1 preceding) sd_term_id_15day_count,

count(sd_retl_id) over (partition by sd_pan order by local_unix_timestamp range between 3600 preceding and 1 preceding) sd_retl_id_1hour_count,
count(sd_retl_id) over (partition by sd_pan order by local_unix_timestamp range between 86400 preceding and 1 preceding) sd_retl_id_1day_count,
count(sd_retl_id) over (partition by sd_pan order by local_unix_timestamp range between 604800 preceding and 1 preceding) sd_retl_id_7day_count,
count(sd_retl_id) over (partition by sd_pan order by local_unix_timestamp range between 1296000 preceding and 1 preceding) sd_retl_id_15day_count,

count(sd_retl_sic_cde) over (partition by sd_pan order by local_unix_timestamp range between 3600 preceding and 1 preceding) sd_retl_sic_cde_1hour_count,
count(sd_retl_sic_cde) over (partition by sd_pan order by local_unix_timestamp range between 86400 preceding and 1 preceding) sd_retl_sic_cde_1day_count,
count(sd_retl_sic_cde) over (partition by sd_pan order by local_unix_timestamp range between 604800 preceding and 1 preceding) sd_retl_sic_cde_7day_count,
count(sd_retl_sic_cde) over (partition by sd_pan order by local_unix_timestamp range between 1296000 preceding and 1 preceding) sd_retl_sic_cde_15day_count,


count(sd_pan) over (partition by sd_retl_id order by local_unix_timestamp range between 3600 preceding and 1 preceding) sd_pan_1hour_count,
count(sd_pan) over (partition by sd_retl_id order by local_unix_timestamp range between 86400 preceding and 1 preceding) sd_pan_1day_count,
count(sd_pan) over (partition by sd_retl_id order by local_unix_timestamp range between 604800 preceding and 1 preceding) sd_pan_7day_count,
count(sd_pan) over (partition by sd_retl_id order by local_unix_timestamp range between 1296000 preceding and 1 preceding) sd_pan_15day_count,