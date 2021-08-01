 
select 
  dd_date,--授权时间
  md_tran_amt1,--消费金额
  sd_acq_inst_id_num,--受理机构代码
  sd_frwd_inst_id_num,--转发机构代码
  sd_pan,--卡号
  sd_trace_nbr,--前置机流水号
  sd_tran_cde_cat,--交易类别
  md_tran_amt3,--交易清算金额
  local_date,
  local_time,
  time_to_radian,
  local_unix_timestamp,
  sd_orig_crncy_cde,      --交易币种
  sd_orig_wir_cntry_cde,  --交易国家代码
  sd_cityno,--交易城市代码
  sd_retl_sic_cde,--商户类型
  sd_retl_id,--商户代码
  china_amt,
  aboard_amt,
  high_risk_country_a_amt,
  high_risk_mcc_code_a_amt,
  max(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding)  rec3times_max_tran_amt,
  stddev(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) rec3times_stddev_tran_amt,
  avg(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) rec3times_avg_tran_amt,
 
  max(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding)  rec7times_max_tran_amt,
  stddev(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) rec7times_stddev_tran_amt,
  avg(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) rec7times_avg_tran_amt,
 
  max(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding)  rec15times_max_tran_amt,
  stddev(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) rec15times_stddev_tran_amt,
  avg(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) rec15times_avg_tran_amt,
  

  max(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) rec3times_max_china_amt,
  avg(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) rec3times_avg_china_amt,
  
  max(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) rec7times_max_china_amt,
  avg(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) rec7times_avg_china_amt,
  
  max(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) rec15times_max_china_amt,
  avg(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) rec15times_avg_china_amt,

   
  max(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) rec3times_max_aboard_amt,
  avg(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) rec3times_avg_aboard_amt,

  max(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) rec7times_max_aboard_amt,
  avg(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) rec7times_avg_aboard_amt,
  
  max(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) rec15times_max_aboard_amt,
  avg(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) rec15times_avg_aboard_amt,
  
  
  max(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) ren3_max_high_risk_country_a_amt,
  avg(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding)
  ren3_avg_high_risk_country_a_amt,
  
  max(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) ren7_max_high_risk_country_a_amt,
  avg(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding)
  ren7_avg_high_risk_country_a_amt,
  
  max(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) ren15_max_high_risk_country_a_amt,
  avg(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding)
  ren15_avg_high_risk_country_a_amt,
    
  max(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) ren3_max_high_risk_mcc_code_a_amt,
  
  avg(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) ren3_avg_high_risk_mcc_code_a_amt,
  
  max(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) ren7_max_high_risk_mcc_code_a_amt,
  avg(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) ren7_avg_high_risk_mcc_code_a_amt,
 
  max(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) ren15_max_high_risk_mcc_code_a_amt,
  avg(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) ren15_avg_high_risk_mcc_code_a_amt,
  
  
  count(md_tran_amt3) over (partition by sd_pan,md_tran_amt3 order  by local_unix_timestamp rows between 3 preceding and 1 preceding) rec3_count_same_tran_amt,
  count(md_tran_amt3) over (partition by sd_pan,sd_cityno order by local_unix_timestamp rows between 3 preceding and 1 preceding)
  rec3_count_same_cityno,
  count(md_tran_amt3) over (partition by sd_pan,sd_retl_id order by local_unix_timestamp rows between 3 preceding and 1 preceding)
  rec3_count_same_retlid,
  
  count(md_tran_amt3) over (partition by sd_pan,md_tran_amt3 order  by local_unix_timestamp rows between 7 preceding and 1 preceding) rec7_count_same_tran_amt,
  count(md_tran_amt3) over (partition by sd_pan,sd_cityno order by local_unix_timestamp rows between 7 preceding and 1 preceding)
  rec7_count_same_cityno,
  count(md_tran_amt3) over (partition by sd_pan,sd_retl_id order by local_unix_timestamp rows between 7 preceding and 1 preceding)
  rec7_count_same_retlid,
  
  count(md_tran_amt3) over (partition by sd_pan,md_tran_amt3 order  by local_unix_timestamp rows between 15 preceding and 1 preceding) rec15_count_same_tran_amt,
  count(md_tran_amt3) over (partition by sd_pan,sd_cityno order by local_unix_timestamp rows between 15 preceding and 1 preceding)
  rec15_count_same_cityno,
  count(md_tran_amt3) over (partition by sd_pan,sd_retl_id order by local_unix_timestamp rows between 15 preceding and 1 preceding)
  rec15_count_same_retlid,
  
  
  case when (sd_orig_wir_cntry_cde - lag(sd_orig_wir_cntry_cde, 1, NULL) over (partition by sd_pan order by local_unix_timestamp))=0
  then 1 else 0 end cntry_cde_befoissame,
  
  case when (lag(sd_orig_wir_cntry_cde, 1, NULL) over (partition by sd_pan order by local_unix_timestamp)) = 
  (lead(sd_orig_wir_cntry_cde, 1, NULL) over (partition by sd_pan order by local_unix_timestamp))
  then 1 else 0 end cntry_cde_neigissame,
  
  md_tran_amt3 / max(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) 
  as  consume_amt_div_max_3times,
  md_tran_amt3 / avg(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 3 preceding and 1 preceding) 
  as  consume_amt_div_avg_3times,
  
  md_tran_amt3 / max(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) 
  as  consume_amt_div_max_7times,
  md_tran_amt3 / avg(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) 
  as  consume_amt_div_avg_7times,

  md_tran_amt3 / max(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) 
  as  consume_amt_div_max_15times,
  md_tran_amt3 / avg(md_tran_amt3) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) 
  as  consume_amt_div_avg_15times,
  
  
  lag(md_tran_amt3, 1, NULL) over (partition by sd_pan order by local_unix_timestamp) rec1times_tran_amt3,
  
  md_tran_amt3 / lag(md_tran_amt3, 1, NULL) over (partition by sd_pan order by local_unix_timestamp) as rec1times_tran_amt3_pec,
  

  lag(china_amt, 1, NULL) over (partition by sd_pan order by local_unix_timestamp) / avg(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) 
  as ren1to7_avg_china_amt_pec,
  
  avg(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) / avg(china_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) as ren7to15_avg_china_amt_pec,
  
  lag(aboard_amt, 1, NULL) over (partition by sd_pan order by local_unix_timestamp)  / avg(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) 
  as ren1to7_avg_aboard_amt_pec,

  avg(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) / avg(aboard_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding)  as ren7to15_avg_aboard_amt_pec,
  
  
  lag(high_risk_country_a_amt, 1, NULL) over (partition by sd_pan order by local_unix_timestamp) / avg(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) as    ren1to7_avg_high_risk_country_a_amt_pec,
  
  avg(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) / avg(high_risk_country_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding) 
  as ren7to15_avg_high_risk_country_a_amt_pec,
  
  lag(high_risk_mcc_code_a_amt, 1, NULL) over (partition by sd_pan order by local_unix_timestamp) / avg(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) ren1to7_avg_high_risk_mcc_code_a_amt_pec,
  
  avg(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 7 preceding and 1 preceding) / avg(high_risk_mcc_code_a_amt) over (partition by sd_pan order by local_unix_timestamp rows between 15 preceding and 1 preceding)
  as ren7to15_avg_high_risk_mcc_code_a_amt_pec
  
  
from
  transaction_flow_pre;
