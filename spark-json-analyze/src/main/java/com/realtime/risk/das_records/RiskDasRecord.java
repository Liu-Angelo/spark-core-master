package com.realtime.risk.das_records;

import java.io.Serializable;

/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/1/16.
 */
public class RiskDasRecord implements Serializable {
    public int request_id;//request表对应ID
    public int account_id;//用户ID(风控)
    public int partner_id;//业务线ID
    public String occupation; //职业类型
    public String company_name; //所在公司
    public String use_mobile_2_cnt_1y; //使用手机号码数(最近一年)
    public String mobile_fixed_days; //最近一年使用手机号码数
    public String adr_stability_days; //地址稳定天数
    public String activity_area_stability; //活动区域个数
    public String have_car_flag; //是否有车
    public String have_fang_flag; //是否有房
    public String last_1m_avg_asset_total; //最近一个月流动资产日均值
    public String last_3m_avg_asset_total; //最近三个月流动资产日均值
    public String last_6m_avg_asset_total; //最近六个月流动资产日均值
    public String last_1y_avg_asset_total; //最近一年流动资产日均值
    public String tot_pay_amt_1m; //最近一个月支付总金额
    public String tot_pay_amt_3m; //最近三个月支付总金额
    public String tot_pay_amt_6m; //最近六个月支付总金额
    public String ebill_pay_amt_1m; //最近一个月消费总金额
    public String ebill_pay_amt_3m; //最近三个月消费总金额
    public String ebill_pay_amt_6m; //最近六个月消费总金额
    public String avg_puc_sdm_last_1y; //最近一年生活缴费层次
    public String pre_1y_pay_cnt; //最近一年手机充值支付总笔数
    public String pre_1y_pay_amount; //最近一年手机充值支付总金额
    public String xfdc_index; //消费档次
    public String credit_pay_amt_1m; //最近一个月信贷类还款总金额
    public String credit_pay_amt_3m; //最近三个月信贷类还款总金额
    public String credit_pay_amt_6m; //最近六个月信贷类还款总金额
    public String credit_pay_amt_1y; //最近一年信贷类还款总金额
    public String credit_pay_months_1y; //最近一年信贷类还款月份数
    public String credit_total_pay_months; //信贷类还款历史月份数
    public String credit_duration; //信用账户历史时长
    public String positive_biz_cnt_1y; //最近一年履约场景数
    public String ovd_order_cnt_2y_m3_status;   //最近两年M3逾期的状态
    public String ovd_order_cnt_2y_m6_status;   //最近两年M6逾期的状态
    public String ovd_order_cnt_3m; //最近三个月逾期总笔数
    public String ovd_order_cnt_3m_m1_status;   //最近三个月M1逾期的状态
    public String ovd_order_cnt_6m_m1_status;   //最近六个月M1逾期的状态
    public String ovd_order_cnt_5y_m3_status;   //最近五年M3逾期的状态
    public String ovd_order_cnt_5y_m6_status;   //最近五年M6逾期的状态
    public String ovd_order_amt_1m; //最近一个月逾期总金额
    public String ovd_order_cnt_12m; //最近一年逾期总笔数
    public String ovd_order_amt_12m; //最近一年逾期总金额
    public String ovd_order_cnt_12m_m1_status;  //最近一年M1逾期的状态
    public String ovd_order_cnt_12m_m3_status;  //最近一年M3逾期的状态
    public String ovd_order_cnt_12m_m6_status;  //最近一年M6逾期的状态
    public String zm_score; //芝麻分
    public String score; //芝麻das评分
    public String scene; //用户请求场景
    public String created_at; //日志生成时间
    public String updated_at; //日志更新时间
    public String load_time; //加载时间

    public Boolean lovesPandas;
}
