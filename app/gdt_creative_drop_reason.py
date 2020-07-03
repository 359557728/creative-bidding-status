# -*- coding: UTF-8 -*-
import os
import treq
import logging
import json
import klein
import random
import time
import string
import hashlib
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Numeric, String, DateTime, Date, UniqueConstraint
from sqlalchemy.sql import text
from twisted.internet import reactor, defer, task
from twisted.web.client import HTTPConnectionPool
from sqlalchemy.dialects.mysql import insert
import pandas as pd
import numpy as np
from logging.handlers import TimedRotatingFileHandler

if not os.path.exists('logs'):
    os.makedirs('logs')

if not os.path.exists('/app/apk'):
    os.makedirs('/app/apk')

handler = TimedRotatingFileHandler(filename='./logs/log.dat', when="midnight", interval=1, encoding='utf8')
handler.suffix = "%Y-%m-%d"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[handler]
)

application = klein.Klein()

BIDDING_FAIL_URL = "https://api.e.qq.com/adx/v1/report/bid_fail"

BIDDING_REPORT_URL = "https://api.e.qq.com/adx/v1/report/bid"

BIDDING_REPORT_URL_MOMO = "https://openad.immomo.com/adx/api/idea/queryReportData"

APP_KEY_MOMO = "cabpochwhxocblxs";

pool = HTTPConnectionPool(reactor)

gdt_advertiser_set = set()

gdt_advertiser_having_cost_set = set()

metadata = MetaData()

tasks_log = Table('yxt_log_synchronization_task', metadata,
    Column('id', Integer(), primary_key=True),
    Column('platform_name', String(10), nullable=False),
    Column('task_name', String(50), nullable=False),
    Column('task_url', String(120), nullable=False),
    Column('state', Integer(), nullable=False),
    Column('start_time', DateTime, default=datetime.datetime.now),
    Column('end_time', DateTime, nullable=True, onupdate=datetime.datetime.now)
)

gdt_advertiser_day_report_table = Table('gdt_daily_report_level_advertiser', metadata,
                                Column('stat_date', Date, primary_key=True),
                                Column('account_id', Integer(), primary_key=True),
                                Column('view_count', Integer(), nullable=True),
                                Column('download_count', Integer(), nullable=True),
                                Column('activated_count', Integer(), nullable=True),
                                Column('activated_rate', Numeric(20, 4), nullable=True),
                                Column('thousand_display_price', Integer(), nullable=True),
                                Column('valid_click_count', Integer(), nullable=True),
                                Column('ctr', Numeric(20, 4), nullable=True),
                                Column('cpc', Integer(), nullable=True),
                                Column('cost', Integer(), nullable=True),
                                Column('key_page_view_cost', Integer(), nullable=True),
                                Column('platform_page_view_count', Integer(), nullable=True),
                                Column('platform_page_view_rate', Numeric(20, 4), nullable=True),
                                Column('web_commodity_page_view_count', Integer(), nullable=True),
                                Column('web_commodity_page_view_cost', Integer(), nullable=True),
                                Column('web_register_count', Integer(), nullable=True),
                                Column('page_consult_count', Integer(), nullable=True),
                                Column('page_consult_cost', Integer(), nullable=True),
                                Column('page_phone_call_direct_count', Integer(), nullable=True),
                                Column('page_phone_call_direct_cost', Integer(), nullable=True),
                                Column('page_phone_call_back_count', Integer(), nullable=True),
                                Column('page_phone_call_back_cost', Integer(), nullable=True),
                                Column('own_page_navigation_count', Integer(), nullable=True),
                                Column('own_page_navi_cost', Integer(), nullable=True),
                                Column('platform_page_navigation_count', Integer(), nullable=True),
                                Column('platform_page_navigation_cost', Integer(), nullable=True),
                                Column('platform_shop_navigation_count', Integer(), nullable=True),
                                Column('platform_shop_navigation_cost', Integer(), nullable=True),
                                Column('web_application_count', Integer(), nullable=True),
                                Column('web_application_cost', Integer(), nullable=True),
                                Column('page_reservation_count', Integer(), nullable=True),
                                Column('page_reservation_rate', Numeric(20, 4), nullable=True),
                                Column('page_reservation_cost', Integer(), nullable=True),
                                Column('add_to_cart_price', Integer(), nullable=True),
                                Column('own_page_coupon_get_count', Integer(), nullable=True),
                                Column('own_page_coupon_get_cost', Integer(), nullable=True),
                                Column('platform_coupon_get_count', Integer(), nullable=True),
                                Column('platform_coupon_get_cost', Integer(), nullable=True),
                                Column('web_order_count', Integer(), nullable=True),
                                Column('web_order_rate', Numeric(20, 4), nullable=True),
                                Column('app_order_rate', Numeric(20, 4), nullable=True),
                                Column('web_order_cost', Integer(), nullable=True),
                                Column('web_checkout_amount', Integer(), nullable=True),
                                Column('web_checkout_count', Integer(), nullable=True),
                                Column('web_checkout_cost', Integer(), nullable=True),
                                Column('order_amount', Integer(), nullable=True),
                                Column('order_unit_price', Integer(), nullable=True),
                                Column('order_roi', Numeric(20, 4), nullable=True),
                                Column('deliver_count', Integer(), nullable=True),
                                Column('deliver_cost', Integer(), nullable=True),
                                Column('sign_in_count', Integer(), nullable=True),
                                Column('sign_in_cost', Integer(), nullable=True),
                                Column('download_rate', Numeric(20, 4), nullable=True),
                                Column('download_cost', Integer(), nullable=True),
                                Column('install_count', Integer(), nullable=True),
                                Column('install_cost', Integer(), nullable=True),
                                Column('click_activated_rate', Numeric(20, 4), nullable=True),
                                Column('activated_cost', Integer(), nullable=True),
                                Column('retention_count', Integer(), nullable=True),
                                Column('retention_rate', Numeric(20, 4), nullable=True),
                                Column('retention_cost', Integer(), nullable=True),
                                Column('key_page_view_count', Integer(), nullable=True),
                                Column('app_commodity_page_view_count', Integer(), nullable=True),
                                Column('app_commodity_page_view_rate', Numeric(20, 4), nullable=True),
                                Column('web_commodity_page_view_rate', Numeric(20, 4), nullable=True),
                                Column('app_commodity_page_view_cost', Integer(), nullable=True),
                                Column('app_register_count', Integer(), nullable=True),
                                Column('app_register_cost', Integer(), nullable=True),
                                Column('web_register_cost', Integer(), nullable=True),
                                Column('app_application_count', Integer(), nullable=True),
                                Column('app_application_cost', Integer(), nullable=True),
                                Column('app_add_to_cart_count', Integer(), nullable=True),
                                Column('add_to_cart_amount', Integer(), nullable=True),
                                Column('app_add_to_cart_cost', Integer(), nullable=True),
                                Column('app_order_count', Integer(), nullable=True),
                                Column('app_order_cost', Integer(), nullable=True),
                                Column('app_checkout_count', Integer(), nullable=True),
                                Column('app_checkout_amount', Integer(), nullable=True),
                                Column('app_checkout_cost', Integer(), nullable=True),
                                Column('platform_coupon_click_count', Integer(), nullable=True),
                                Column('platform_coupon_get_rate', Numeric(20, 4), nullable=True),
                                Column('follow_count', Integer(), nullable=True),
                                Column('follow_cost', Integer(), nullable=True),
                                Column('forward_count', Integer(), nullable=True),
                                Column('forward_cost', Integer(), nullable=True),
                                Column('read_count', Integer(), nullable=True),
                                Column('read_cost', Integer(), nullable=True),
                                Column('praise_count', Integer(), nullable=True),
                                Column('praise_cost', Integer(), nullable=True),
                                Column('comment_count', Integer(), nullable=True),
                                Column('comment_cost', Integer(), nullable=True),
                                Column('inte_phone_count', Integer(), nullable=True),
                                Column('phone_call_count', Integer(), nullable=True),
                                Column('external_form_reservation_count', Integer(), nullable=True),
                                Column('ad_pur_val_web', Integer(), nullable=True),
                                Column('ad_pur_val_app', Integer(), nullable=True),
                                Column('game_create_role_count', Integer(), nullable=True),
                                Column('game_authorize_count', Integer(), nullable=True),
                                Column('game_tutorial_finish_count', Integer(), nullable=True),
                                Column('effective_leads_count', Integer(), nullable=True),
                                Column('effective_cost', Integer(), nullable=True),
                                Column('effective_reserve_count', Integer(), nullable=True),
                                Column('effective_consult_count', Integer(), nullable=True),
                                Column('effective_phone_count', Integer(), nullable=True),
                                Column('potential_reserve_count', Integer(), nullable=True),
                                Column('potential_consult_count', Integer(), nullable=True),
                                Column('potential_phone_count', Integer(), nullable=True),
                                Column('app_checkout_rate', Numeric(20, 4), nullable=True),
                                Column('web_checkout_rate', Numeric(20, 4), nullable=True),
                                Column('app_activated_checkout_rate', Numeric(20, 4), nullable=True),
                                Column('web_activated_checkout_rate', Numeric(20, 4), nullable=True),
                                Column('app_register_rate', Numeric(20, 4), nullable=True),
                                Column('web_reg_rate', Numeric(20, 4), nullable=True),
                                Column('page_phone_call_direct_rate', Numeric(20, 4), nullable=True),
                                Column('page_phone_call_back_rate', Numeric(20, 4), nullable=True),
                                Column('page_consult_rate', Numeric(20, 4), nullable=True),
                                Column('deliver_rate', Numeric(20, 4), nullable=True),
                                Column('install_rate', Numeric(20, 4), nullable=True),
                                Column('arppu_cost', Integer(), nullable=True),
                                Column('arpu_cost', Integer(), nullable=True),
                                Column('cheout_fd', Integer(), nullable=True),
                                Column('cheout_td', Integer(), nullable=True),
                                Column('cheout_ow', Integer(), nullable=True),
                                Column('cheout_tw', Integer(), nullable=True),
                                Column('cheout_om', Integer(), nullable=True),
                                Column('cheout_fd_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_td_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_ow_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_tw_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_om_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_total_reward', Numeric(20, 4), nullable=True),
                                Column('first_pay_count', Integer(), nullable=True),
                                Column('first_pay_rate', Numeric(20, 4), nullable=True),
                                Column('pre_cre_web', Integer(), nullable=True),
                                Column('pre_cre_app', Integer(), nullable=True),
                                Column('pre_cre_web_val', Integer(), nullable=True),
                                Column('pre_cre_app_val', Integer(), nullable=True),
                                Column('cre_web', Integer(), nullable=True),
                                Column('cre_app', Integer(), nullable=True),
                                Column('cre_web_val', Integer(), nullable=True),
                                Column('cre_app_val', Integer(), nullable=True),
                                Column('withdr_dep_web', Integer(), nullable=True),
                                Column('withdr_dep_app', Integer(), nullable=True),
                                Column('withdr_dep_web_val', Integer(), nullable=True),
                                Column('withdr_dep_app_val', Integer(), nullable=True),
                                Column('first_pay_cost', Integer(), nullable=True),
                                Column('landing_page_click_count', Integer(), nullable=True),
                                Column('scan_follow_count', Integer(), nullable=True),
                                Column('web_cart_amount', Integer(), nullable=True),
                                Column('app_order_amount', Integer(), nullable=True),
                                Column('web_order_amount', Integer(), nullable=True),
                                Column('phone_consult_count', Integer(), nullable=True),
                                Column('tool_consult_count', Integer(), nullable=True),
                                Column('lottery_leads_count', Integer(), nullable=True),
                                Column('lottery_leads_cost', Integer(), nullable=True),
                                Column('conversions_count', Integer(), nullable=True),
                                Column('conversions_rate', Numeric(20, 4), nullable=True),
                                Column('conversions_cost', Integer(), nullable=True),
                                Column('deep_conversions_count', Integer(), nullable=True),
                                Column('deep_conversions_rate', Numeric(20, 4), nullable=True),
                                Column('deep_conversions_cost', Integer(), nullable=True),
                                Column('first_memcard_web_count', Integer(), nullable=True),
                                Column('first_memcard_app_count', Integer(), nullable=True),
                                Column('memcard_web_count', Integer(), nullable=True),
                                Column('memcard_app_count', Integer(), nullable=True),
                                Column('first_memcard_web_rate', Numeric(20, 4), nullable=True),
                                Column('first_memcard_app_rate', Numeric(20, 4), nullable=True),
                                Column('first_memcard_web_cost', Integer(), nullable=True),
                                Column('first_memcard_app_cost', Integer(), nullable=True),
                                Column('valuable_click_count', Integer(), nullable=True),
                                Column('valuable_click_rate', Numeric(20, 4), nullable=True),
                                Column('valuable_click_cost', Integer(), nullable=True),
                                Column('overall_leads_purchase_count', Integer(), nullable=True),
                                Column('leads_purchase_count', Integer(), nullable=True),
                                Column('leads_purchase_rate', Numeric(20, 4), nullable=True),
                                Column('leads_purchase_cost', Integer(), nullable=True),
                                Column('leads_purchase_uv', Integer(), nullable=True),
                                Column('valid_leads_uv', Integer(), nullable=True),
                                Column('phone_call_uv', Integer(), nullable=True),
                                Column('valid_phone_uv', Integer(), nullable=True),
                                Column('potential_customer_phone_uv', Integer(), nullable=True),
                                Column('web_register_uv', Integer(), nullable=True),
                                Column('web_apply_uv', Integer(), nullable=True),
                                Column('web_credit_uv', Integer(), nullable=True),
                                Column('app_apply_uv', Integer(), nullable=True),
                                Column('app_pre_credit_uv', Integer(), nullable=True),
                                Column('app_credit_uv', Integer(), nullable=True),
                                Column('app_withdraw_uv', Integer(), nullable=True),
                                Column('wechat_app_register_uv', Integer(), nullable=True),
                                Column('first_day_order_count', Integer(), nullable=True),
                                Column('first_day_order_amount', Integer(), nullable=True),
                                Column('add_wishlist_count', Integer(), nullable=True),
                                Column('video_outer_play_count', Integer(), nullable=True),
                                Column('click_detail_count', Integer(), nullable=True),
                                Column('click_head_count', Integer(), nullable=True),
                                Column('click_image_count', Integer(), nullable=True),
                                Column('click_nick_count', Integer(), nullable=True),
                                Column('click_poi_count', Integer(), nullable=True),
                                Column('cpn_click_button_count', Integer(), nullable=True),
                                Column('cpn_click_button_uv', Integer(), nullable=True),
                                Column('key_page_uv', Integer(), nullable=True),
                                Column('no_interest_count', Integer(), nullable=True),
                                Column('special_page_exp_cost', Integer(), nullable=True),
                                Column('special_page_exp_uv', Integer(), nullable=True),
                                Column('video_inner_play_count', Integer(), nullable=True),
                                Column('video_play_count', Integer(), nullable=True),
                                Column('view_commodity_page_uv', Integer(), nullable=True),
                                Column('web_add_to_cart_cost', Integer(), nullable=True),
                                Column('web_add_to_cart_count', Integer(), nullable=True)
                             )

gdt_advertiser_hour_report_table = Table('gdt_hourly_report_level_advertiser', metadata,
                                Column('stat_date', Date, primary_key=True),
                                Column('account_id', Integer(), primary_key=True),
                                Column('hour', Integer(), primary_key=True),
                                Column('view_count', Integer(), nullable=True),
                                Column('download_count', Integer(), nullable=True),
                                Column('activated_count', Integer(), nullable=True),
                                Column('activated_rate', Numeric(20, 4), nullable=True),
                                Column('thousand_display_price', Integer(), nullable=True),
                                Column('valid_click_count', Integer(), nullable=True),
                                Column('ctr', Numeric(20, 4), nullable=True),
                                Column('cpc', Integer(), nullable=True),
                                Column('cost', Integer(), nullable=True),
                                Column('key_page_view_cost', Integer(), nullable=True),
                                Column('platform_page_view_count', Integer(), nullable=True),
                                Column('platform_page_view_rate', Numeric(20, 4), nullable=True),
                                Column('web_commodity_page_view_count', Integer(), nullable=True),
                                Column('web_commodity_page_view_cost', Integer(), nullable=True),
                                Column('web_register_count', Integer(), nullable=True),
                                Column('page_consult_count', Integer(), nullable=True),
                                Column('page_consult_cost', Integer(), nullable=True),
                                Column('page_phone_call_direct_count', Integer(), nullable=True),
                                Column('page_phone_call_direct_cost', Integer(), nullable=True),
                                Column('page_phone_call_back_count', Integer(), nullable=True),
                                Column('page_phone_call_back_cost', Integer(), nullable=True),
                                Column('own_page_navigation_count', Integer(), nullable=True),
                                Column('own_page_navi_cost', Integer(), nullable=True),
                                Column('platform_page_navigation_count', Integer(), nullable=True),
                                Column('platform_page_navigation_cost', Integer(), nullable=True),
                                Column('platform_shop_navigation_count', Integer(), nullable=True),
                                Column('platform_shop_navigation_cost', Integer(), nullable=True),
                                Column('web_application_count', Integer(), nullable=True),
                                Column('web_application_cost', Integer(), nullable=True),
                                Column('page_reservation_count', Integer(), nullable=True),
                                Column('page_reservation_rate', Numeric(20, 4), nullable=True),
                                Column('page_reservation_cost', Integer(), nullable=True),
                                Column('add_to_cart_price', Integer(), nullable=True),
                                Column('own_page_coupon_get_count', Integer(), nullable=True),
                                Column('own_page_coupon_get_cost', Integer(), nullable=True),
                                Column('platform_coupon_get_count', Integer(), nullable=True),
                                Column('platform_coupon_get_cost', Integer(), nullable=True),
                                Column('web_order_count', Integer(), nullable=True),
                                Column('web_order_rate', Numeric(20, 4), nullable=True),
                                Column('app_order_rate', Numeric(20, 4), nullable=True),
                                Column('web_order_cost', Integer(), nullable=True),
                                Column('web_checkout_amount', Integer(), nullable=True),
                                Column('web_checkout_count', Integer(), nullable=True),
                                Column('web_checkout_cost', Integer(), nullable=True),
                                Column('order_amount', Integer(), nullable=True),
                                Column('order_unit_price', Integer(), nullable=True),
                                Column('order_roi', Numeric(20, 4), nullable=True),
                                Column('deliver_count', Integer(), nullable=True),
                                Column('deliver_cost', Integer(), nullable=True),
                                Column('sign_in_count', Integer(), nullable=True),
                                Column('sign_in_cost', Integer(), nullable=True),
                                Column('download_rate', Numeric(20, 4), nullable=True),
                                Column('download_cost', Integer(), nullable=True),
                                Column('install_count', Integer(), nullable=True),
                                Column('install_cost', Integer(), nullable=True),
                                Column('click_activated_rate', Numeric(20, 4), nullable=True),
                                Column('activated_cost', Integer(), nullable=True),
                                Column('retention_count', Integer(), nullable=True),
                                Column('retention_rate', Numeric(20, 4), nullable=True),
                                Column('retention_cost', Integer(), nullable=True),
                                Column('key_page_view_count', Integer(), nullable=True),
                                Column('app_commodity_page_view_count', Integer(), nullable=True),
                                Column('app_commodity_page_view_rate', Numeric(20, 4), nullable=True),
                                Column('web_commodity_page_view_rate', Numeric(20, 4), nullable=True),
                                Column('app_commodity_page_view_cost', Integer(), nullable=True),
                                Column('app_register_count', Integer(), nullable=True),
                                Column('app_register_cost', Integer(), nullable=True),
                                Column('web_register_cost', Integer(), nullable=True),
                                Column('app_application_count', Integer(), nullable=True),
                                Column('app_application_cost', Integer(), nullable=True),
                                Column('app_add_to_cart_count', Integer(), nullable=True),
                                Column('add_to_cart_amount', Integer(), nullable=True),
                                Column('app_add_to_cart_cost', Integer(), nullable=True),
                                Column('app_order_count', Integer(), nullable=True),
                                Column('app_order_cost', Integer(), nullable=True),
                                Column('app_checkout_count', Integer(), nullable=True),
                                Column('app_checkout_amount', Integer(), nullable=True),
                                Column('app_checkout_cost', Integer(), nullable=True),
                                Column('platform_coupon_click_count', Integer(), nullable=True),
                                Column('platform_coupon_get_rate', Numeric(20, 4), nullable=True),
                                Column('follow_count', Integer(), nullable=True),
                                Column('follow_cost', Integer(), nullable=True),
                                Column('forward_count', Integer(), nullable=True),
                                Column('forward_cost', Integer(), nullable=True),
                                Column('read_count', Integer(), nullable=True),
                                Column('read_cost', Integer(), nullable=True),
                                Column('praise_count', Integer(), nullable=True),
                                Column('praise_cost', Integer(), nullable=True),
                                Column('comment_count', Integer(), nullable=True),
                                Column('comment_cost', Integer(), nullable=True),
                                Column('inte_phone_count', Integer(), nullable=True),
                                Column('phone_call_count', Integer(), nullable=True),
                                Column('external_form_reservation_count', Integer(), nullable=True),
                                Column('ad_pur_val_web', Integer(), nullable=True),
                                Column('ad_pur_val_app', Integer(), nullable=True),
                                Column('game_create_role_count', Integer(), nullable=True),
                                Column('game_authorize_count', Integer(), nullable=True),
                                Column('game_tutorial_finish_count', Integer(), nullable=True),
                                Column('effective_leads_count', Integer(), nullable=True),
                                Column('effective_cost', Integer(), nullable=True),
                                Column('effective_reserve_count', Integer(), nullable=True),
                                Column('effective_consult_count', Integer(), nullable=True),
                                Column('effective_phone_count', Integer(), nullable=True),
                                Column('potential_reserve_count', Integer(), nullable=True),
                                Column('potential_consult_count', Integer(), nullable=True),
                                Column('potential_phone_count', Integer(), nullable=True),
                                Column('app_checkout_rate', Numeric(20, 4), nullable=True),
                                Column('web_checkout_rate', Numeric(20, 4), nullable=True),
                                Column('app_activated_checkout_rate', Numeric(20, 4), nullable=True),
                                Column('web_activated_checkout_rate', Numeric(20, 4), nullable=True),
                                Column('app_register_rate', Numeric(20, 4), nullable=True),
                                Column('web_reg_rate', Numeric(20, 4), nullable=True),
                                Column('page_phone_call_direct_rate', Numeric(20, 4), nullable=True),
                                Column('page_phone_call_back_rate', Numeric(20, 4), nullable=True),
                                Column('page_consult_rate', Numeric(20, 4), nullable=True),
                                Column('deliver_rate', Numeric(20, 4), nullable=True),
                                Column('install_rate', Numeric(20, 4), nullable=True),
                                Column('arppu_cost', Integer(), nullable=True),
                                Column('arpu_cost', Integer(), nullable=True),
                                Column('cheout_fd', Integer(), nullable=True),
                                Column('cheout_td', Integer(), nullable=True),
                                Column('cheout_ow', Integer(), nullable=True),
                                Column('cheout_tw', Integer(), nullable=True),
                                Column('cheout_om', Integer(), nullable=True),
                                Column('cheout_fd_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_td_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_ow_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_tw_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_om_reward', Numeric(20, 4), nullable=True),
                                Column('cheout_total_reward', Numeric(20, 4), nullable=True),
                                Column('first_pay_count', Integer(), nullable=True),
                                Column('first_pay_rate', Numeric(20, 4), nullable=True),
                                Column('pre_cre_web', Integer(), nullable=True),
                                Column('pre_cre_app', Integer(), nullable=True),
                                Column('pre_cre_web_val', Integer(), nullable=True),
                                Column('pre_cre_app_val', Integer(), nullable=True),
                                Column('cre_web', Integer(), nullable=True),
                                Column('cre_app', Integer(), nullable=True),
                                Column('cre_web_val', Integer(), nullable=True),
                                Column('cre_app_val', Integer(), nullable=True),
                                Column('withdr_dep_web', Integer(), nullable=True),
                                Column('withdr_dep_app', Integer(), nullable=True),
                                Column('withdr_dep_web_val', Integer(), nullable=True),
                                Column('withdr_dep_app_val', Integer(), nullable=True),
                                Column('first_pay_cost', Integer(), nullable=True),
                                Column('landing_page_click_count', Integer(), nullable=True),
                                Column('scan_follow_count', Integer(), nullable=True),
                                Column('web_cart_amount', Integer(), nullable=True),
                                Column('app_order_amount', Integer(), nullable=True),
                                Column('web_order_amount', Integer(), nullable=True),
                                Column('phone_consult_count', Integer(), nullable=True),
                                Column('tool_consult_count', Integer(), nullable=True),
                                Column('lottery_leads_count', Integer(), nullable=True),
                                Column('lottery_leads_cost', Integer(), nullable=True),
                                Column('conversions_count', Integer(), nullable=True),
                                Column('conversions_rate', Numeric(20, 4), nullable=True),
                                Column('conversions_cost', Integer(), nullable=True),
                                Column('deep_conversions_count', Integer(), nullable=True),
                                Column('deep_conversions_rate', Numeric(20, 4), nullable=True),
                                Column('deep_conversions_cost', Integer(), nullable=True),
                                Column('first_memcard_web_count', Integer(), nullable=True),
                                Column('first_memcard_app_count', Integer(), nullable=True),
                                Column('memcard_web_count', Integer(), nullable=True),
                                Column('memcard_app_count', Integer(), nullable=True),
                                Column('first_memcard_web_rate', Numeric(20, 4), nullable=True),
                                Column('first_memcard_app_rate', Numeric(20, 4), nullable=True),
                                Column('first_memcard_web_cost', Integer(), nullable=True),
                                Column('first_memcard_app_cost', Integer(), nullable=True),
                                Column('valuable_click_count', Integer(), nullable=True),
                                Column('valuable_click_rate', Numeric(20, 4), nullable=True),
                                Column('valuable_click_cost', Integer(), nullable=True),
                                Column('overall_leads_purchase_count', Integer(), nullable=True),
                                Column('leads_purchase_count', Integer(), nullable=True),
                                Column('leads_purchase_rate', Numeric(20, 4), nullable=True),
                                Column('leads_purchase_cost', Integer(), nullable=True),
                                Column('leads_purchase_uv', Integer(), nullable=True),
                                Column('valid_leads_uv', Integer(), nullable=True),
                                Column('phone_call_uv', Integer(), nullable=True),
                                Column('valid_phone_uv', Integer(), nullable=True),
                                Column('potential_customer_phone_uv', Integer(), nullable=True),
                                Column('web_register_uv', Integer(), nullable=True),
                                Column('web_apply_uv', Integer(), nullable=True),
                                Column('web_credit_uv', Integer(), nullable=True),
                                Column('app_apply_uv', Integer(), nullable=True),
                                Column('app_pre_credit_uv', Integer(), nullable=True),
                                Column('app_credit_uv', Integer(), nullable=True),
                                Column('app_withdraw_uv', Integer(), nullable=True),
                                Column('wechat_app_register_uv', Integer(), nullable=True),
                                Column('first_day_order_count', Integer(), nullable=True),
                                Column('first_day_order_amount', Integer(), nullable=True),
                                Column('add_wishlist_count', Integer(), nullable=True),
                                Column('video_outer_play_count', Integer(), nullable=True),
                                Column('click_detail_count', Integer(), nullable=True),
                                Column('click_head_count', Integer(), nullable=True),
                                Column('click_image_count', Integer(), nullable=True),
                                Column('click_nick_count', Integer(), nullable=True),
                                Column('click_poi_count', Integer(), nullable=True),
                                Column('cpn_click_button_count', Integer(), nullable=True),
                                Column('cpn_click_button_uv', Integer(), nullable=True),
                                Column('key_page_uv', Integer(), nullable=True),
                                Column('no_interest_count', Integer(), nullable=True),
                                Column('special_page_exp_cost', Integer(), nullable=True),
                                Column('special_page_exp_uv', Integer(), nullable=True),
                                Column('video_inner_play_count', Integer(), nullable=True),
                                Column('video_play_count', Integer(), nullable=True),
                                Column('view_commodity_page_uv', Integer(), nullable=True),
                                UniqueConstraint('stat_date','account_id','hour', name='idx_data_account_hour')
                             )

engine_orm = create_engine('mysql+pymysql://user:password@ip:port/dbname', pool_pre_ping=True, pool_recycle=36000)

metadata.create_all(engine_orm)

@application.route('/momo/bid/report/<string:startDate>/<string:endDate>')
def momo_bidding_report(request, startDate, endDate):
    uptime = int(time.time())
    original = 'appkey' + APP_KEY_MOMO + 'uptime' + str(uptime)
    md5Lib = hashlib.md5()
    md5Lib.update(original.encode("utf8"))
    sign = md5Lib.hexdigest().upper()
    request_data = json.dumps({'dspid' : "yima", 'sign' : sign, 'uptime' : uptime, 'start_time': startDate, 'end_time': endDate})
    logging.info('date report of query parameter send to momo {}'.format('data=' + request_data))
    deferred = treq.post(BIDDING_REPORT_URL_MOMO, data = {'data' : request_data}, pool=pool)
    deferred.addCallback(momo_bid_report_done)
    return deferred

def momo_bid_report_done(response):
    deferred = treq.text_content(response, encoding="utf-8")
    deferred.addCallback(momo_bid_report_received)
    deferred.addErrback(lambda x: None) # ignore errors
    return deferred

def momo_bid_report_received(data):
    logging.info('bid report of momo data received {}'.format(data))
    try:
        maybe_continuable = json.loads(data)
        if maybe_continuable['ec'] == 200:
            records = [data for data in maybe_continuable['data'] if data['bids'] > 0]
            logging.info('bid report of momo data received then filtered {}'.format(records))
            records = maybe_continuable['data']
            sql = """INSERT INTO channel_consumption (stat_day, channel_id, type, adx_bid, adx_winbid, adx_impression, adx_valid_impression, adx_click, adx_valid_click, adx_cost) VALUES (:stat_day, 5995, 2, :adx_bid, :adx_winbid, :adx_impression, :adx_valid_impression, :adx_click, :adx_valid_click, :adx_cost) ON DUPLICATE KEY UPDATE adx_bid = :adx_bid, adx_winbid = :adx_winbid, adx_impression = :adx_impression, adx_valid_impression = :adx_valid_impression, adx_click = :adx_click, adx_valid_click = :adx_valid_click, adx_cost = :adx_cost"""
            lines = [{'stat_day' : item['time'], 'adx_bid' : item['bids'], 'adx_winbid' : item['bids_win'], 'adx_impression' : item['display'], 'adx_valid_impression' : item['uniq_display'], 'adx_click' : item['click'], 'adx_valid_click' : item['uniq_click'], 'adx_cost' : item['fee']} for item in records]
            batch_insert(sql, lines)
    except Exception as e:
        logging.error(e)
    return data

@application.route('/android/apk/url/parse', methods=['POST'])
def apk_parse(request):
    apk_link = json.loads(request.content.read().decode('utf-8'))['apk_link']
    logging.info('apk from {}'.format(apk_link))
    file_name = 'yxt' + str(random.randint(100000000000000, 9999999999999999)) + '.apk'
    deferred = download_file(reactor, apk_link, '/app/apk/' + file_name)
    deferred.addCallback(file_path_saved, file_name)
    request.responseHeaders.addRawHeader(b"content-type", b"application/json")
    return deferred

def download_file(reactor, url, destination_filename):
    destination = open(destination_filename, 'wb')
    deferred = treq.get(url)
    deferred.addCallback(treq.collect, destination.write)
    deferred.addBoth(lambda _: destination.close)
    return deferred

def file_path_saved(nodata, destination_filename):
    logging.info('apk saved into path {} with data {}'.format(destination_filename, nodata))
    return json.dumps({"apk_path" : destination_filename})

def ran_str(size):
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))

@application.route('/yxt/customer/total/snapshot/create')
async def yxt_customer_total(request):
    request.responseHeaders.addRawHeader(b"content-type", b"application/json")
    get_column = lambda df, col : df.get(col, pd.Series(index = df.index, name = col))
    engine_dsp_prod = create_engine('mysql+pymysql://user:password@ip:port/dbname', pool_pre_ping=True, pool_recycle=36000)
    #去掉 ep.pig_platform_id = 1 logic copy from admin
    #亿信推现金充值
    sql_yxt_cash_deposit = """
        SELECT
            customer.id AS customer_id,
            DATE_FORMAT(
                deposit.deposit_time,
                '%%Y-%%m-%%d'
            ) stat_date,
            IFNULL(
                SUM(deposit.deposit_amount),
                0
            ) deposit_yxt_cash
        FROM
            emarbox_project ep
        INNER JOIN pig_project_deposit_log deposit ON ep.project_id = deposit.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
            customer.id IS NOT NULL
        AND deposit.deposit_time IS NOT NULL
        GROUP BY
            customer.id,
            DATE_FORMAT(
                deposit.deposit_time,
                '%%Y-%%m-%%d'
            )
            """
    #去掉 ep.pig_platform_id = 1 logic copy from admin
    #亿信推返点充值
    sql_yxt_gift_deposit = """
        SELECT
            customer.id AS customer_id,
            DATE_FORMAT(
                present.present_time,
                '%%Y-%%m-%%d'
            ) stat_date,
            IFNULL(
                SUM(present.present_amount),
                0
            ) deposit_yxt_gift
        FROM
            emarbox_project ep
        INNER JOIN pig_project_present_log present ON ep.project_id = present.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
            present.present_time IS NOT NULL
        AND customer.id IS NOT NULL
        GROUP BY
            customer.id,
            DATE_FORMAT(
                present.present_time,
                '%%Y-%%m-%%d'
            )
    """
    #广点通充值
    sql_gdt_deposit = """
        SELECT
            customer.id AS customer_id,
            DATE_FORMAT(t.trade_time, '%%Y-%%m-%%d') AS stat_date,
            ifnull(sum(t.amount) / 100, 0) AS deposit_all_gdt
        FROM
            gdt_cash_flow t
        LEFT JOIN gdt_advertiser ga ON t.uid = ga.uid
        LEFT JOIN emarbox_project ep ON ga.project_id = ep.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
            trade_type = 'CHARGE'
        AND t.account_type IN (
                        'FUND_TYPE_CASH',
                        'FUND_TYPE_GIFT',
                        'FUND_TYPE_CREDIT_ROLL',
                        'FUND_TYPE_COMPENSATE_VIRTUAL'
                    )
        AND ep.pig_platform_id = 2
        AND customer.id IS NOT NULL
        GROUP BY
            customer.id,
            t.trade_time
    """
    #头条充值
    sql_tt_deposit = """
        SELECT
            customer.id AS customer_id,
            DATE_FORMAT(t.create_time, '%%Y-%%m-%%d') AS stat_date,
            ifnull(sum(t.amount), 0) AS deposit_all_tt
        FROM
            tt_fund_transaction_desc t
        LEFT JOIN tt_advertiser tt ON t.advertiser_id = tt.advertiser_id
        LEFT JOIN emarbox_project ep ON tt.project_id = ep.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
            t.transaction_type = 'TRANSFER' AND t.remitter IN (52927843638, 6728224549)
        AND ep.pig_platform_id = 3
        AND customer.id IS NOT NULL
        GROUP BY
            customer.id,
            DATE_FORMAT(t.create_time, '%%Y-%%m-%%d')
    """
    #微信充值
    sql_wechat_deposit = """
        SELECT
            customer.id as customer_id,
            DATE_FORMAT(FROM_UNIXTIME(t.wechat_time, '%%Y-%%m-%%d'), '%%Y-%%m-%%d') AS stat_date,
            ifnull(sum(t.amount), 0) / 100 AS deposit_all_wechat
        FROM
            gdt_wechat_fund_statements_detailed t
        INNER JOIN pig_wechat_project_info info ON t.wechat_account_id = info.wechat_account_id
        LEFT JOIN emarbox_project ep ON info.project_id = ep.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
            t.trade_type IN (
                'AGENCY_TRANSFER_TO_ADVERTISER'
            )
        AND ep.pig_platform_id = 4
        AND customer.id IS NOT NULL
        GROUP BY
            customer.id,
            FROM_UNIXTIME(t.wechat_time, '%%Y-%%m-%%d')
    """
    #广点通消耗 -- 资金账户级别 可以用于计算成本
    sql_gdt_consume_customer_date_account_type = """
        SELECT
            sum(IFNULL(t.amount, 0) / 100) cost,
            customer.id customer_id,
            t.account_type,
            DATE_FORMAT(t.trade_time, '%%Y-%%m-%%d') stat_date
        FROM
            gdt_cash_flow t
        INNER JOIN gdt_advertiser ga ON t.uid = ga.uid
        INNER JOIN emarbox_project ep ON ga.project_id = ep.project_id
        INNER JOIN yxt_finance_customer_entity adv_info ON ep.pig_advertiser_id = adv_info.id
        INNER JOIN yxt_finance_customer customer ON adv_info.customer_id = customer.id
        WHERE
            t.trade_type = 'PAY'
        AND t.account_type IN (
            'FUND_TYPE_CASH',
            'FUND_TYPE_GIFT',
            'FUND_TYPE_CREDIT_ROLL',
            'FUND_TYPE_COMPENSATE_VIRTUAL'
        )
        GROUP BY
            customer.id,
            t.account_type,
            t.trade_time
    """
    #微信消耗
    sql_wechat_consume_consumer_date = """
        SELECT
            sum(IFNULL(t.cost,0)/100) total_cost_wechat,
            customer.id customer_id,
            DATE_FORMAT(t.date, '%%Y-%%m-%%d') stat_date
        FROM
            gdt_wechat_daily_reports_advertiser t
        INNER JOIN pig_wechat_project_info ga ON t.wechat_account_id = ga.wechat_account_id
        INNER JOIN emarbox_project ep ON ga.project_id = ep.project_id
        INNER JOIN yxt_finance_customer_entity adv_info ON ep.pig_advertiser_id = adv_info.id
        INNER JOIN yxt_finance_customer customer ON adv_info.customer_id = customer.id
        GROUP BY customer.id, t.date
    """
    #头条消耗 -- 包含了现金消耗可用于计算成本
    sql_tt_consume_consumer_date = """
        SELECT
            sum(IFNULL(t.cash_cost, 0)) cash_cost_tt,
            sum(IFNULL(t.cash_cost,0)) +  sum(IFNULL(t.reward_cost,0)) total_cost_tt,
            customer.id customer_id,
            DATE_FORMAT(t.date, '%%Y-%%m-%%d') stat_date
        FROM
            tt_fund_daily_stat t
        INNER JOIN tt_advertiser ga ON t.advertiser_id = ga.advertiser_id
        INNER JOIN emarbox_project ep ON ga.project_id = ep.project_id
        INNER JOIN yxt_finance_customer_entity adv_info ON ep.pig_advertiser_id = adv_info.id
        INNER JOIN yxt_finance_customer customer ON adv_info.customer_id = customer.id
        GROUP BY
            customer.id,
            t.date
    """
    #亿信推消耗
    sql_yxt_consume_consumer_date = """
        SELECT
        	sum(IFNULL(cs_log.pay_consume_amount,0)) + sum(
        		IFNULL(cs_log.present_consume_amount,0)
        	) total_cost_yxt,
        	customer.id customer_id,
        	DATE_FORMAT(
        		cs_log.consume_time,
        		'%%Y-%%m-%%d'
        	) stat_date
        FROM
        	pig_campaign_consume_log cs_log
        INNER JOIN emarbox_project ep ON cs_log.project_id = ep.project_id
        INNER JOIN yxt_finance_customer_entity adv_info ON ep.pig_advertiser_id = adv_info.id
        INNER JOIN yxt_finance_customer customer ON adv_info.customer_id = customer.id
        GROUP BY
        	customer.id,
        	date(cs_log.consume_time)
    """
    #微信退款
    sql_wechat_refund = """
        SELECT
            customer.id AS customer_id,
            FROM_UNIXTIME(t.wechat_time, '%%Y-%%m-%%d') as stat_date,
            ifnull(sum(t.amount ), 0) / 100 AS refund_all_wechat
        FROM
            gdt_wechat_fund_statements_detailed t
        INNER JOIN pig_wechat_project_info info ON t.wechat_account_id = info.wechat_account_id
        LEFT JOIN emarbox_project ep ON info.project_id = ep.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
            t.trade_type IN (
                'AGENCY_REFUND_FROM_ADVERTISER'
            )
        AND ep.pig_platform_id = 4 AND customer.id IS NOT NULL
        GROUP BY
            customer.id,
            FROM_UNIXTIME(t.wechat_time, '%%Y-%%m-%%d')
    """
    #头条退款
    sql_tt_refund = """
        SELECT
            customer.id AS customer_id,
            DATE_FORMAT(t.create_time, '%%Y-%%m-%%d') AS stat_date,
            ifnull(sum(t.amount), 0) AS refund_all_tt
        FROM
            tt_fund_transaction_desc t
        INNER JOIN tt_advertiser tt ON t.advertiser_id = tt.advertiser_id
        LEFT JOIN emarbox_project ep ON tt.project_id = ep.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
            t.transaction_type = 'TRANSFER' AND t.payee  IN (52927843638, 6728224549)
        AND ep.pig_platform_id = 3
        AND customer.id IS NOT NULL
        GROUP BY
            customer.id,
            DATE_FORMAT(t.create_time, '%%Y-%%m-%%d')
    """
    #广点通退款
    sql_gdt_refund = """
        SELECT
            customer.id AS customer_id,
            DATE_FORMAT(t.trade_time, '%%Y-%%m-%%d') AS stat_date,
            ifnull(sum(t.amount) / 100, 0) AS refund_all_gdt
        FROM
            gdt_cash_flow t
        LEFT JOIN gdt_advertiser ga ON t.uid = ga.uid
        LEFT JOIN emarbox_project ep ON ga.project_id = ep.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
            trade_type = 'TRANSFER_BACK'
        AND ep.pig_platform_id = 2
        AND customer.id IS NOT NULL
        GROUP BY
            customer.id,
            t.trade_time
    """
    #亿信推现金退款
    sql_yxt_cash_refund = """
        SELECT
            customer.id AS customer_id,
            DATE_FORMAT(
                refund.refund_time,
                '%%Y-%%m-%%d'
            ) stat_date,
            IFNULL(
                SUM(refund.refund_amount),
                0
            ) refund_yxt_cash
        FROM
            emarbox_project ep
        INNER JOIN pig_project_refund_log refund ON ep.project_id = refund.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
        customer.id IS NOT NULL
        GROUP BY
            customer.id,
            DATE_FORMAT(
                refund.refund_time,
                '%%Y-%%m-%%d'
            )
    """
    #亿信推返点退款
    sql_yxt_gift_refund = """
        SELECT
            customer.id AS customer_id,
            DATE_FORMAT(
                refund.gift_refund_time,
                '%%Y-%%m-%%d'
            ) stat_date,
            IFNULL(
                SUM(refund.gift_refund_amount),
                0
            ) refund_yxt_gift
        FROM
            emarbox_project ep
        INNER JOIN pig_project_gift_refund_log refund ON ep.project_id = refund.project_id
        LEFT JOIN yxt_finance_customer_entity entity ON entity.id = ep.pig_advertiser_id
        LEFT JOIN yxt_finance_customer customer ON customer.id = entity.customer_id
        WHERE
        customer.id IS NOT NULL
        GROUP BY
            customer.id,
            DATE_FORMAT(
                refund.gift_refund_time,
                '%%Y-%%m-%%d'
            )
    """
    # media_id find in yxt_finance_platform_media only support media_id of 1,2,3,4
    sql_yxt_contract = """
        SELECT
            b.customer_entity_id,
            a.id AS contract_content_id,
            a.media_id AS platform_id,
            case b.settle_type when 2 then IFNULL(a.contract_rebate, 0) * 0.0001 else 0 end gift_ratio,
            a.contract_begin_date AS contract_begin_date,
            a.contract_end_date AS contract_end_date
        FROM
            yxt_finance_contract_content a
        INNER JOIN yxt_finance_contract b ON a.contract_id = b.id
        WHERE
            a.remark NOT LIKE '%%测试%%' and a.media_id in (1,2,3,4)
        ORDER BY
            customer_entity_id
    """
    sql_gdt_consume_customer_entity_date = """
        SELECT
            sum(IFNULL(t.amount, 0) / 100) cost,
            customer.id customer_id,
            adv_info.id AS customer_entity_id,
            DATE_FORMAT(t.trade_time, '%%Y-%%m-%%d') stat_date
        FROM
            gdt_cash_flow t
        INNER JOIN gdt_advertiser ga ON t.uid = ga.uid
        INNER JOIN emarbox_project ep ON ga.project_id = ep.project_id
        INNER JOIN yxt_finance_customer_entity adv_info ON ep.pig_advertiser_id = adv_info.id
        INNER JOIN yxt_finance_customer customer ON adv_info.customer_id = customer.id
        WHERE
            t.trade_type = 'PAY'
        AND t.account_type IN (
            'FUND_TYPE_CASH',
            'FUND_TYPE_GIFT',
            'FUND_TYPE_CREDIT_ROLL'
        )
        GROUP BY
            customer.id,
            adv_info.id,
            t.trade_time
    """
    sql_tt_consume_customer_entity_date = """
        SELECT
            IFNULL(SUM(t.reward_cost), 0) + IFNULL(SUM(t.cash_cost), 0) cost,
            customer.id customer_id,
            adv_info.id AS customer_entity_id,
            DATE_FORMAT(t.date, '%%Y-%%m-%%d') stat_date
        FROM
            tt_fund_daily_stat t
        INNER JOIN tt_advertiser ga ON t.advertiser_id = ga.advertiser_id
        INNER JOIN emarbox_project ep ON ga.project_id = ep.project_id
        INNER JOIN yxt_finance_customer_entity adv_info ON ep.pig_advertiser_id = adv_info.id
        INNER JOIN yxt_finance_customer customer ON adv_info.customer_id = customer.id
        GROUP BY
            customer.id,
            adv_info.id,
            t.date
    """
    sql_wechat_consume_customer_entity_date = """
        SELECT
            sum(IFNULL(t.cost, 0) / 100) cost,
            customer.id customer_id,
            adv_info.id AS customer_entity_id,
            DATE_FORMAT(t.date, '%%Y-%%m-%%d') stat_date
        FROM
            gdt_wechat_daily_reports_advertiser t
        INNER JOIN pig_wechat_project_info ga ON t.wechat_account_id = ga.wechat_account_id
        INNER JOIN emarbox_project ep ON ga.project_id = ep.project_id
        INNER JOIN yxt_finance_customer_entity adv_info ON ep.pig_advertiser_id = adv_info.id
        INNER JOIN yxt_finance_customer customer ON adv_info.customer_id = customer.id
        GROUP BY
            customer.id,
            adv_info.id,
            t.date
    """
    sql_yxt_consume_customer_entity_date = """
        SELECT
            IFNULL(
                sum(cs_log.pay_consume_amount),
                0
            ) + IFNULL(
                sum(
                    cs_log.present_consume_amount
                ),
                0
            ) cost,
            customer.id customer_id,
            adv_info.id customer_entity_id,
            DATE_FORMAT(
                cs_log.consume_time,
                '%%Y-%%m-%%d'
            ) stat_date
        FROM
            pig_campaign_consume_log cs_log
        INNER JOIN emarbox_project ep ON cs_log.project_id = ep.project_id
        INNER JOIN yxt_finance_customer_entity adv_info ON ep.pig_advertiser_id = adv_info.id
        INNER JOIN yxt_finance_customer customer ON adv_info.customer_id = customer.id
        GROUP BY
            customer.id,
            adv_info.id,
            date(cs_log.consume_time)
    """
    #开票
    sql_yxt_invoice = """
        SELECT
            SUM(
                IFNULL(a.invoice_balance, 0)
            ) AS invoice_amount,
            a.customer_id,
            DATE_FORMAT(
                a.instance_finish_time,
                '%%Y-%%m-%%d'
            ) stat_date
        FROM
            pig_dd_invoice_process_flow a
        LEFT JOIN pig_dd_process_instance_common b ON a.process_instance_id = b.process_instance_code
        WHERE
            a.result = 'agree'
        AND a.`status` = 'COMPLETED'
        AND a.customer_id IS NOT NULL
        GROUP BY
            a.customer_id,
            DATE_FORMAT(
                a.instance_finish_time,
                '%%Y-%%m-%%d'
            )
    """
    #到款
    sql_yxt_payback = """
        SELECT
            b.customer_id,
            DATE_FORMAT(receipt_date, '%%Y-%%m-%%d') AS stat_date,
            SUM(IFNULL(a.receipt_money, 0)) AS arrive_amount
        FROM
            yxt_finance_arrival_money_order_contract a
        INNER JOIN yxt_finance_arrival_money_order b ON a.arrival_money_order_id = b.id
        INNER JOIN yxt_finance_customer_entity d ON d.customer_id = b.customer_id
        AND d.id = b.customer_entity_id
        WHERE
            a.is_delete = 0
        GROUP BY
            b.customer_id,
            DATE_FORMAT(receipt_date, '%%Y-%%m-%%d')
    """
    #亿信推成本
    sql_yxt_cost_cb = """
        SELECT
            SUM(IFNULL(a.dsp_cost, 0)) AS cb_yxt,
            c.customer_id,
            a.stat_date
        FROM
            (
                SELECT
                    project_id,
                    date(stat_date) AS stat_date,
                    sum(IFNULL(dsp_cost, 0)) AS dsp_cost
                FROM
                    campaign_report_detail
                WHERE
                    stat_date >= '2019-03-01 00:00:00'
                GROUP BY
                    project_id,
                    date(stat_date)
            ) a
        INNER JOIN emarbox_project b ON a.project_id = b.project_id
        INNER JOIN yxt_finance_customer_entity c ON c.id = b.pig_advertiser_id
        INNER JOIN yxt_finance_customer d ON d.id = c.customer_id
        WHERE
            c.customer_id IS NOT NULL
        GROUP BY
            c.customer_id,
            a.stat_date
    """
    sql_customer_info = """
        SELECT
            id AS customer_id
        FROM
            yxt_finance_customer
        ORDER BY
            id
    """
    sql_billed_contract_account = """
        SELECT
            b.kpi_start_date,
            b.kpi_end_date,
            DATE_FORMAT(d.contract_begin_date, '%%Y-%%m-%%d') as contract_begin_date,
            DATE_FORMAT(d.contract_end_date, '%%Y-%%m-%%d') as contract_end_date,
            a.pig_platform_id,
            c.customer_entity_id,
            b.result,
            b.`status`,
            b.billed_type,
            a.account_id_platform,
            IFNULL(cash_consumed,0) cash_consumed,
            IFNULL(cash_deposit,0) cash_deposit,
            IFNULL(gift_consumed,0) gift_consumed,
            IFNULL(gift_deposit,0) gift_deposit,
            IFNULL(credit_consumed,0) credit_consumed,
            IFNULL(credit_deposit,0) credit_deposit,
            IFNULL(coupon_consumed,0) coupon_consumed,
            IFNULL(coupon_deposit,0) coupon_deposit,
            a.process_instance_id,
            a.business_id
        FROM
            (
                SELECT
                    CASE platform_name
                WHEN '今日头条' THEN
                    3
                WHEN '广点通' THEN
                    2
                WHEN '亿信推' THEN
                    1
                WHEN '微信' THEN
                    4
                ELSE
                    0
                END AS pig_platform_id,
                process_instance_id,
                account_id_platform,
                business_id,
                cash_consumed,
                cash_deposit,
                gift_consumed,
                gift_deposit,
                credit_consumed,
                credit_deposit,
                coupon_consumed,
                coupon_deposit
            FROM
                pig_dd_billed_process_flow_detail
            ) a
        INNER JOIN pig_dd_billed_process_flow b ON a.process_instance_id = b.process_instance_id
        AND a.business_id = b.business_id
        INNER JOIN yxt_finance_contract c ON b.contract_info = c.id
        INNER JOIN yxt_finance_contract_content d ON d.contract_id = c.id
        WHERE
            a.pig_platform_id = d.media_id
    """
    with engine_dsp_prod.connect() as conn, conn.begin():
        yxt_cash_deposit = pd.read_sql(sql_yxt_cash_deposit, conn)
        # yxt_cash_deposit contain column name - customer_id, stat_date, deposit_yxt_cash
        if not yxt_cash_deposit.empty:
            yxt_cash_deposit = yxt_cash_deposit.groupby(['customer_id', 'stat_date']).agg(sum)
            yxt_cash_deposit = yxt_cash_deposit.reset_index()
        else:
            yxt_cash_deposit = pd.DataFrame(columns=['customer_id', 'stat_date', 'deposit_yxt_cash'])
        yxt_gift_deposit = pd.read_sql(sql_yxt_gift_deposit, conn)
        # yxt_gift_deposit contain column name - customer_id, stat_date, deposit_yxt_gift
        if not yxt_gift_deposit.empty:
            yxt_gift_deposit = yxt_gift_deposit.groupby(['customer_id', 'stat_date']).agg(sum)
            yxt_gift_deposit = yxt_gift_deposit.reset_index()
        else:
            yxt_gift_deposit = pd.DataFrame(columns=['customer_id', 'stat_date', 'deposit_yxt_gift'])
        # gdt_deposit contain column name - customer_id, stat_date, deposit_all_gdt
        gdt_deposit = pd.read_sql(sql_gdt_deposit, conn)
        # tt_deposit contain column name - customer_id, stat_date, deposit_all_tt
        tt_deposit = pd.read_sql(sql_tt_deposit, conn)
        # wechat_deposit contain column name - customer_id, stat_date, deposit_all_wechat
        wechat_deposit = pd.read_sql(sql_wechat_deposit, conn)
        # gdt_consume_customer_date_account_type contain column name - customer_id,stat_date,account_type,total_cost_gdt
        gdt_consume_customer_date_account_type = pd.read_sql(sql_gdt_consume_customer_date_account_type, conn)
        gdt_consume_customer_date = gdt_consume_customer_date_account_type.groupby(['customer_id', 'stat_date']).agg(sum)
        gdt_consume_customer_date.rename(columns = {'cost': 'total_cost_gdt'}, inplace = True)
        # gdt_consume_customer_date contain column name - customer_id,stat_date,total_cost_gdt
        gdt_consume_customer_date = gdt_consume_customer_date.reset_index()
        # wechat_consume_customer_date contain column name - customer_id,stat_date,total_cost_wechat
        wechat_consume_customer_date = pd.read_sql(sql_wechat_consume_consumer_date, conn)
        # tt_consume_customer_date contain column name - customer_id,stat_date,total_cost_tt,cash_cost_tt
        tt_consume_customer_date = pd.read_sql(sql_tt_consume_consumer_date, conn)
        # yxt_consume_customer_date contain column name - customer_id,stat_date,total_cost_yxt
        yxt_consume_customer_date = pd.read_sql(sql_yxt_consume_consumer_date, conn)
        #gdt_refund contain column name - customer_id,stat_date,refund_all_gdt
        gdt_refund = pd.read_sql(sql_gdt_refund, conn)
        #tt_refund contain column name - customer_id,stat_date,refund_all_tt
        tt_refund = pd.read_sql(sql_tt_refund, conn)
        #wechat_refund contain column name - customer_id,stat_date,refund_all_wechat
        wechat_refund = pd.read_sql(sql_wechat_refund, conn)
        #yxt_gift_refund contain column name - customer_id,stat_date,refund_yxt_gift
        yxt_gift_refund = pd.read_sql(sql_yxt_gift_refund, conn)
        #yxt_cash_refund contain column name - customer_id,stat_date,refund_yxt_cash
        yxt_cash_refund = pd.read_sql(sql_yxt_cash_refund, conn)
        #yxt_invoice contain column name - customer_id,stat_date,invoice_amount-开票金额
        yxt_invoice = pd.read_sql(sql_yxt_invoice, conn)
        #yxt_payback contain column name - customer_id,stat_date,arrive_amount
        yxt_payback = pd.read_sql(sql_yxt_payback, conn)
        gdt_consume_customer_date_account_type_cb = gdt_consume_customer_date_account_type.loc[gdt_consume_customer_date_account_type['account_type'].isin(['FUND_TYPE_CASH', 'FUND_TYPE_CREDIT_ROLL'])]
        gdt_consume_customer_date_cost_cb = gdt_consume_customer_date_account_type_cb.groupby(['customer_id', 'stat_date']).agg(sum)
        gdt_consume_customer_date_cost_cb.rename(columns = {'cost': 'cb_gdt'}, inplace = True)
        # gdt_consume_customer_date_cost_cb contains columns - 	customer_id	stat_date cb_gdt
        gdt_consume_customer_date_cost_cb = gdt_consume_customer_date_cost_cb.reset_index()
        # -- tt_consume_customer_date contains tt cb as well as cost
        # -- wechat_consume_customer_date contains only have one cost
        # yxt_consume_customer_date_cost_cb contains columns customer_id, stat_date, cb_yxt
        yxt_consume_customer_date_cost_cb = pd.read_sql(sql_yxt_cost_cb, conn)
        yxt_customer_id = pd.read_sql(sql_customer_info, conn)
        date_till_yesterday = pd.date_range('2020-01-01', datetime.date.today().strftime("%Y-%m-%d"), freq="1D").strftime("%Y-%m-%d")
        customer_id_cross_date = pd.MultiIndex.from_product([yxt_customer_id['customer_id'], date_till_yesterday], names = ['customer_id', 'stat_date'])
        customer_id_cross_date = customer_id_cross_date.to_frame()
        customer_id_cross_date = customer_id_cross_date.reset_index(drop = True)
        yxt_cash_deposit_append = pd.merge(customer_id_cross_date, yxt_cash_deposit, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_gift_deposit_append = pd.merge(yxt_cash_deposit_append, yxt_gift_deposit, on = ['customer_id', 'stat_date'], how = 'left')
        gdt_deposit_append = pd.merge(yxt_gift_deposit_append, gdt_deposit, on = ['customer_id', 'stat_date'], how = 'left')
        tt_deposit_append = pd.merge(gdt_deposit_append, tt_deposit, on = ['customer_id', 'stat_date'], how = 'left')
        wechat_deposit_append = pd.merge(tt_deposit_append, wechat_deposit, on = ['customer_id', 'stat_date'], how = 'left')
        gdt_refund_append = pd.merge(wechat_deposit_append, gdt_refund, on = ['customer_id', 'stat_date'], how = 'left')
        tt_refund_append = pd.merge(gdt_refund_append, tt_refund, on = ['customer_id', 'stat_date'], how = 'left')
        wechat_refund_append = pd.merge(tt_refund_append, wechat_refund, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_gift_refund_append = pd.merge(wechat_refund_append, yxt_gift_refund, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_cash_refund_append = pd.merge(yxt_gift_refund_append, yxt_cash_refund, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_invoice_append = pd.merge(yxt_cash_refund_append, yxt_invoice, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_payback_append = pd.merge(yxt_invoice_append, yxt_payback, on = ['customer_id', 'stat_date'], how = 'left')
        gdt_consume_customer_date_append = pd.merge(yxt_payback_append, gdt_consume_customer_date, on = ['customer_id', 'stat_date'], how = 'left')
        tt_consume_customer_date_append = pd.merge(gdt_consume_customer_date_append, tt_consume_customer_date, on = ['customer_id', 'stat_date'], how = 'left')
        wechat_consume_customer_date_append = pd.merge(tt_consume_customer_date_append, wechat_consume_customer_date, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_consume_customer_date_append = pd.merge(wechat_consume_customer_date_append, yxt_consume_customer_date, on = ['customer_id', 'stat_date'], how = 'left')
        # 成本 append start
        # gdt 成本 append
        gdt_consume_customer_date_cost_cb_append = pd.merge(yxt_consume_customer_date_append, gdt_consume_customer_date_cost_cb, on = ['customer_id', 'stat_date'], how = 'left')
        # tt 成本 append
        gdt_consume_customer_date_cost_cb_append.rename(columns = {'cash_cost_tt': 'cb_tt'}, inplace = True)
        # yxt 成本 append
        yxt_consume_customer_date_cost_cb_append = pd.merge(gdt_consume_customer_date_cost_cb_append, yxt_consume_customer_date_cost_cb, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_consume_customer_date_cost_cb_append = yxt_consume_customer_date_cost_cb_append.fillna(0)
        # yxt total refund calculate
        yxt_consume_customer_date_cost_cb_append['refund_all_yxt'] = get_column(yxt_consume_customer_date_cost_cb_append, 'refund_yxt_gift').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'refund_yxt_cash').fillna(0)
        # all platform refund
        yxt_consume_customer_date_cost_cb_append['refund_all'] = get_column(yxt_consume_customer_date_cost_cb_append, 'refund_all_yxt').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'refund_all_wechat').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'refund_all_gdt').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'refund_all_tt').fillna(0)
        # yxt total deposit calculate
        yxt_consume_customer_date_cost_cb_append['deposit_all_yxt'] = get_column(yxt_consume_customer_date_cost_cb_append, 'deposit_yxt_cash').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'deposit_yxt_gift').fillna(0)
        # all platform deposit
        yxt_consume_customer_date_cost_cb_append['deposit_all'] = get_column(yxt_consume_customer_date_cost_cb_append, 'deposit_all_yxt').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'deposit_all_wechat').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'deposit_all_gdt').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'deposit_all_tt').fillna(0)
        # all platform total cost
        yxt_consume_customer_date_cost_cb_append['total_cost_all'] = get_column(yxt_consume_customer_date_cost_cb_append, 'total_cost_yxt').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'total_cost_wechat').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'total_cost_gdt').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'total_cost_tt').fillna(0)
        # all platform cb
        yxt_consume_customer_date_cost_cb_append['cb_all'] = get_column(yxt_consume_customer_date_cost_cb_append, 'cb_yxt').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'cb_wechat').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'cb_gdt').fillna(0) + get_column(yxt_consume_customer_date_cost_cb_append, 'cb_tt').fillna(0)
        logging.info("cb append")
        # contract info
        yxt_contract = pd.read_sql(sql_yxt_contract, conn)
        mapping = {3 : 4, 4 : 3}
        yxt_contract['platform_id'].replace(mapping, inplace = True)
        yxt_contract = yxt_contract.melt(id_vars=['customer_entity_id', 'gift_ratio', 'platform_id', 'contract_content_id'], value_vars = ['contract_begin_date', 'contract_end_date'], var_name = 'contract_date_type', value_name = 'contract_date')
        yxt_contract['contract_date'] = yxt_contract['contract_date'].apply(pd.to_datetime)
        yxt_contract = yxt_contract.set_index('contract_date').groupby(['customer_entity_id', 'platform_id', 'contract_content_id']).apply(lambda x : x.resample('D').max().ffill().drop(['customer_entity_id', 'platform_id', 'contract_content_id'], axis = 1)).reset_index(level=1)
        yxt_contract = yxt_contract.reset_index()
        yxt_contract = yxt_contract.drop_duplicates(['customer_entity_id', 'platform_id', 'contract_content_id', 'contract_date'])
        yxt_contract.rename(columns = {'contract_date': 'stat_date'}, inplace = True)
        # convert contract_date('stat_date') to str
        yxt_contract['stat_date'] = yxt_contract['stat_date'].dt.strftime('%Y-%m-%d')
        gdt_consume_customer_entity_date = pd.read_sql(sql_gdt_consume_customer_entity_date, conn)
        gdt_incoming = pd.merge(gdt_consume_customer_entity_date, yxt_contract.loc[yxt_contract['platform_id'] == 2], on = ['customer_entity_id', 'stat_date'], how = 'left')
        gdt_incoming['gift_ratio'].fillna(0, inplace = True)
        gdt_incoming['incoming_gdt'] = gdt_incoming['cost'] / (gdt_incoming['gift_ratio'] + 1)
        gdt_incoming = gdt_incoming.groupby(['customer_id', 'stat_date']).agg({'incoming_gdt': sum})
        gdt_incoming = gdt_incoming.reset_index()
        gdt_incoming_append = pd.merge(yxt_consume_customer_date_cost_cb_append, gdt_incoming, on = ['customer_id', 'stat_date'], how = 'left')
        tt_consume_customer_entity_date = pd.read_sql(sql_tt_consume_customer_entity_date, conn)
        tt_incoming = pd.merge(tt_consume_customer_entity_date, yxt_contract.loc[yxt_contract['platform_id'] == 3], on = ['customer_entity_id', 'stat_date'], how = 'left')
        tt_incoming['gift_ratio'].fillna(0, inplace = True)
        tt_incoming['incoming_tt'] = tt_incoming['cost'] / (tt_incoming['gift_ratio'] + 1)
        tt_incoming = tt_incoming.groupby(['customer_id', 'stat_date']).agg({'incoming_tt': sum})
        tt_incoming = tt_incoming.reset_index()
        tt_incoming_append = pd.merge(gdt_incoming_append, tt_incoming, on = ['customer_id', 'stat_date'], how = 'left')
        wechat_consume_customer_entity_date = pd.read_sql(sql_wechat_consume_customer_entity_date, conn)
        wechat_incoming = pd.merge(wechat_consume_customer_entity_date, yxt_contract.loc[yxt_contract['platform_id'] == 4], on = ['customer_entity_id', 'stat_date'], how = 'left')
        wechat_incoming['gift_ratio'].fillna(0, inplace = True)
        wechat_incoming['incoming_wechat'] = wechat_incoming['cost'] / (wechat_incoming['gift_ratio'] + 1)
        wechat_incoming = wechat_incoming.groupby(['customer_id', 'stat_date']).agg({'incoming_wechat': sum})
        wechat_incoming = wechat_incoming.reset_index()
        wechat_incoming_append = pd.merge(tt_incoming_append, wechat_incoming, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_consume_customer_entity_date = pd.read_sql(sql_yxt_consume_customer_entity_date, conn)
        yxt_incoming = pd.merge(yxt_consume_customer_entity_date, yxt_contract.loc[yxt_contract['platform_id'] == 1], on = ['customer_entity_id', 'stat_date'], how = 'left')
        yxt_incoming['gift_ratio'].fillna(0, inplace = True)
        yxt_incoming['incoming_yxt'] = yxt_incoming['cost'] / (yxt_incoming['gift_ratio'] + 1)
        yxt_incoming = yxt_incoming.groupby(['customer_id', 'stat_date']).agg({'incoming_yxt': sum})
        yxt_incoming = yxt_incoming.reset_index()
        yxt_incoming_append = pd.merge(wechat_incoming_append, yxt_incoming, on = ['customer_id', 'stat_date'], how = 'left')
        yxt_incoming_append.fillna(0, inplace = True)
        yxt_incoming_append['incoming_all'] = get_column(yxt_incoming_append, 'incoming_gdt').fillna(0) + get_column(yxt_incoming_append, 'incoming_tt').fillna(0) + get_column(yxt_incoming_append, 'incoming_wechat').fillna(0) + get_column(yxt_incoming_append, 'incoming_yxt').fillna(0)
        #已开票未回款
        yxt_incoming_append['invoice_without_arrival'] = yxt_incoming_append['invoice_amount'] - yxt_incoming_append['arrive_amount']
        #已消耗未回款
        yxt_incoming_append['conusme_without_arrival'] = yxt_incoming_append['incoming_all'] - yxt_incoming_append['arrive_amount']
        #已消耗未开票
        yxt_incoming_append['conusme_without_invoice'] = yxt_incoming_append['incoming_all'] - yxt_incoming_append['invoice_amount']
        engine_dev = create_engine('mysql+pymysql://user:password@ip:port/dbname', pool_pre_ping=True, pool_recycle=36000)
        with engine_dev.connect() as conn_dev, conn_dev.begin():
            yxt_incoming_append[:].to_sql('yxt_customer_summary_statics', conn_dev, if_exists='append', method= 'multi', chunksize=2000, index=False)
            logging.info('yxt_incoming_append export complete')
        logging.info(yxt_incoming_append.loc[(yxt_incoming_append['customer_id'] == 620) & (yxt_incoming_append['stat_date'] == '2020-05-23')].iloc[0])
        # 结算单与合同信息 yxt_billed_contract which contains
        '''yxt_billed_contract_account = pd.read_sql(sql_billed_contract_account, conn)
        # 结算单,合同,代理商广告主ID进行关联
        yxt_billed_contract_account = yxt_billed_contract_account.groupby(['process_instance_id', 'business_id', 'pig_platform_id', 'account_id_platform']).nth(0)
        yxt_billed_contract_account = yxt_billed_contract_account.reset_index()'''
        logging.info("invoice")
        if not yxt_invoice.empty:
            logging.info(yxt_invoice.iloc[0])
        else:
            logging.info("invoice empty")
        logging.info("payback")
        if not yxt_payback.empty:
            logging.info(yxt_payback.iloc[0])
        else:
            logging.info("payback empty")
        return json.dumps({'code' : 0})

@application.route('/gdt/hour/advertiser/report/<string:date>')
async def gdt_advertiser_hour_report(request, date):
    request.responseHeaders.addRawHeader(b"content-type", b"application/json")
    task_id = task_create(date)
    logging.info('gdt advertiser hour report get start')
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    date = date if date else today
    token_rsp = await treq.get("http://social.emarbox.com/gdt/token/access")
    token = await token_rsp.content()
    advertiser_id_batch_got = await advertiser_id_batch_retrieve(1, token)
    advertiser_list = []
    if advertiser_id_batch_got and advertiser_id_batch_got['data']['list']:
        advertiser_list_first_page = [advertiser_single['account_id'] for advertiser_single in advertiser_id_batch_got['data']['list']]
        advertiser_list += advertiser_list_first_page
    if advertiser_id_batch_got and advertiser_id_batch_got['data']['page_info']['total_page'] > 1:
        page_total = list(range(2, advertiser_id_batch_got['data']['page_info']['total_page'] + 1))
        advertiser_id_total_deferred = [defer.ensureDeferred(advertiser_id_batch_retrieve(page_num, token)) for page_num in page_total]
        advertiser_id_total_deferred_resolved = await defer.gatherResults(advertiser_id_total_deferred)
        advertiser_id_total_deferred_resolved_extract = [single_page['account_id'] for advertiser_single in advertiser_id_total_deferred_resolved if advertiser_single and advertiser_single['data']['list'] for single_page in advertiser_single['data']['list']]
        advertiser_list += advertiser_id_total_deferred_resolved_extract
    gdt_advertiser_set.update(advertiser_list)
    logging.info("advertiser id total [%s]" % (gdt_advertiser_set))
    advertiser_list = list(gdt_advertiser_set)
    advertiser_list_chunks = [advertiser_list[x : x + 100] for x in range(0, len(advertiser_list), 100)]
    single_advertiser_hour_data_total = []
    for advertiser_list_sub in advertiser_list_chunks:
        advertiser_hour_report_total_deferred = [defer.ensureDeferred(single_advertiser_id_hour_report_retrieve(advertiser_id, token, date)) for advertiser_id in advertiser_list_sub]
        advertiser_hour_report_total_deferred_resolved = await defer.gatherResults(advertiser_hour_report_total_deferred)
        advertiser_hour_report_total_deferred_resolved_extract = [single_page for advertiser_single in advertiser_hour_report_total_deferred_resolved if advertiser_single for single_page in advertiser_single]
        single_advertiser_hour_data_total += advertiser_hour_report_total_deferred_resolved_extract
        logging.info("gdt hour report complete")
    if single_advertiser_hour_data_total:
        df_hour_report = pd.DataFrame.from_dict(single_advertiser_hour_data_total)
        advertiser_list_having_cost = df_hour_report[df_hour_report['cost'] > 0]['account_id'].drop_duplicates().to_list()
        logging.info("advertiser id having hour cost [%s]" % (advertiser_list_having_cost))
        if advertiser_list_having_cost:
            gdt_advertiser_having_cost_set.update(advertiser_list_having_cost)
    task_finish(task_id)
    return json.dumps(single_advertiser_hour_data_total)

async def single_page_gdt_advertiser_hour_report(advertiser_id, page, token, date):
    fields_hour_report = ['hour','view_count','download_count','activated_count','activated_rate','thousand_display_price','valid_click_count','ctr','cpc','cost','key_page_view_cost','platform_page_view_count','platform_page_view_rate','web_commodity_page_view_count','web_commodity_page_view_cost','web_register_count','page_consult_count','page_consult_cost','page_phone_call_direct_count','page_phone_call_direct_cost','page_phone_call_back_count','page_phone_call_back_cost','own_page_navigation_count','own_page_navi_cost','platform_page_navigation_count','platform_page_navigation_cost','platform_shop_navigation_count','platform_shop_navigation_cost','web_application_count','web_application_cost','page_reservation_count','page_reservation_rate','page_reservation_cost','add_to_cart_price','own_page_coupon_get_count','own_page_coupon_get_cost','platform_coupon_get_count','platform_coupon_get_cost','web_order_count','web_order_rate','app_order_rate','web_order_cost','web_checkout_amount','web_checkout_count','web_checkout_cost','order_amount','order_unit_price','order_roi','deliver_count','deliver_cost','sign_in_count','sign_in_cost','download_rate','download_cost','install_count','install_cost','click_activated_rate','activated_cost','retention_count','retention_rate','retention_cost','key_page_view_count','app_commodity_page_view_count','app_commodity_page_view_rate','web_commodity_page_view_rate','app_commodity_page_view_cost','app_register_count','app_register_cost','web_register_cost','app_application_count','app_application_cost','app_add_to_cart_count','add_to_cart_amount','app_add_to_cart_cost','app_order_count','app_order_cost','app_checkout_count','app_checkout_amount','app_checkout_cost','platform_coupon_click_count','platform_coupon_get_rate','follow_count','follow_cost','forward_count','forward_cost','read_count','read_cost','praise_count','praise_cost','comment_count','comment_cost','inte_phone_count','phone_call_count','external_form_reservation_count','ad_pur_val_web','ad_pur_val_app','game_create_role_count','game_authorize_count','game_tutorial_finish_count','effective_leads_count','effective_cost','effective_reserve_count','effective_consult_count','effective_phone_count','potential_reserve_count','potential_consult_count','potential_phone_count','app_checkout_rate','web_checkout_rate','app_activated_checkout_rate','web_activated_checkout_rate','app_register_rate','web_reg_rate','page_phone_call_direct_rate','page_phone_call_back_rate','page_consult_rate','deliver_rate','install_rate','arppu_cost','arpu_cost','cheout_fd','cheout_td','cheout_ow','cheout_tw','cheout_om','cheout_fd_reward','cheout_td_reward','cheout_ow_reward','cheout_tw_reward','cheout_om_reward','cheout_total_reward','first_pay_count','first_pay_rate','pre_cre_web','pre_cre_app','pre_cre_web_val','pre_cre_app_val','cre_web','cre_app','cre_web_val','cre_app_val','withdr_dep_web','withdr_dep_app','withdr_dep_web_val','withdr_dep_app_val','first_pay_cost','landing_page_click_count','scan_follow_count','web_cart_amount','app_order_amount','web_order_amount','phone_consult_count','tool_consult_count','lottery_leads_count','lottery_leads_cost','conversions_count','conversions_rate','conversions_cost','deep_conversions_count','deep_conversions_rate','deep_conversions_cost','first_memcard_web_count','first_memcard_app_count','memcard_web_count','memcard_app_count','first_memcard_web_rate','first_memcard_app_rate','first_memcard_web_cost','first_memcard_app_cost','valuable_click_count','valuable_click_rate','valuable_click_cost','video_play_count','click_image_count','click_detail_count','click_head_count','click_nick_count','click_poi_count','video_inner_play_count','cpn_click_button_count','cpn_click_button_uv','key_page_uv','special_page_exp_uv','special_page_exp_cost','view_commodity_page_uv','overall_leads_purchase_count','leads_purchase_count','leads_purchase_rate','leads_purchase_cost','leads_purchase_uv','valid_leads_uv','phone_call_uv','valid_phone_uv','potential_customer_phone_uv','web_register_uv','web_apply_uv','web_credit_uv','app_apply_uv','app_pre_credit_uv','app_credit_uv','app_withdraw_uv','wechat_app_register_uv','no_interest_count','first_day_order_count','first_day_order_amount','add_wishlist_count','video_outer_play_count']
    date_range={"start_date" : date, "end_date" : date}
    group_by = ["hour"]
    order_by = [{"sort_field" : "hour", "sort_type" : "ASCENDING"}]
    hour_report_url = 'https://api.e.qq.com/v1.1/hourly_reports/get?access_token=' + token.decode('utf-8') + '&timestamp=' + str(int(time.time())) + '&nonce=' + ran_str(31) + "&fields=" + json.dumps(fields_hour_report) + "&page=" + str(page) + "&page_size=100" + "&account_id=" + str(advertiser_id) + "&level=REPORT_LEVEL_ADVERTISER" + "&date_range=" + json.dumps(date_range) + "&group_by=" + json.dumps(group_by) + "&order_by=" + json.dumps(order_by)
    hour_res = await treq.get(hour_report_url)
    hour_data = await hour_res.content()
    advertiser_hour_data = json.loads(hour_data.decode('utf-8'))
    if 'code' in advertiser_hour_data and 'data' in advertiser_hour_data and advertiser_hour_data['code'] == 0 and advertiser_hour_data['data']['list']:
        return advertiser_hour_data
    else:
        logging.info("hour data code [%s] message [%s] date [%s] advertiser id [%s] page [%s]" % (advertiser_hour_data.get('code'), advertiser_hour_data.get('message'), date, advertiser_id, page))
        return {}

async def advertiser_id_batch_retrieve(page, token):
    fields = ['account_id', 'corporation_name', 'system_status']
    advertiser_url = 'https://api.e.qq.com/v1.1/advertiser/get?access_token=' + token.decode('utf-8') + '&timestamp=' + str(int(time.time())) + '&nonce=' + ran_str(31) + "&fields=" + json.dumps(fields) + "&page=" + str(page) + "&page_size=100"
    advertiser_res = await treq.get(advertiser_url)
    advertiser = await advertiser_res.content()
    data = json.loads(advertiser.decode('utf-8'))
    if 'code' in data and 'data' in data and data['code'] == 0 and data['data']['list']:
        return data
    else:
        logging.info("advertiser id code [%s] message [%s] page [%s]" % (data.get('code'), data.get('message'), page))
        return {}

async def single_advertiser_id_hour_report_retrieve(advertiser_id, token, date):
    logging.info("advertiser id [%d] is processing" % (advertiser_id))
    single_advertiser_hour_data = []
    single_gdt_advertiser_hour_report_single_page = await single_page_gdt_advertiser_hour_report(advertiser_id, 1, token, date)
    if single_gdt_advertiser_hour_report_single_page and single_gdt_advertiser_hour_report_single_page['data']['list']:
        single_advertiser_hour_data += single_gdt_advertiser_hour_report_single_page['data']['list']
        logging.info("advertiser id [%d] hour report first page entered" % (advertiser_id))
    if single_gdt_advertiser_hour_report_single_page and single_gdt_advertiser_hour_report_single_page['data']['page_info']['total_page'] > 1:
        page_total = list(range(2, single_gdt_advertiser_hour_report_single_page['data']['page_info']['total_page'] + 1))
        logging.info("advertiser id [%d] hour report second page entered" % (advertiser_id))
        advertiser_total_deferred = [defer.ensureDeferred(single_page_gdt_advertiser_hour_report(advertiser_id, page, token, date)) for page in page_total]
        advertiser_total_deferred_resolved = await defer.gatherResults(advertiser_total_deferred)
        advertiser_total_deferred_resolved_extract = [single_page for advertiser_single in advertiser_total_deferred_resolved if advertiser_single and advertiser_single['data']['list'] for single_page in advertiser_single['data']['list']]
        single_advertiser_hour_data += advertiser_total_deferred_resolved_extract
    if single_advertiser_hour_data:
        total_data = [advertiser_id_date_append(advertiser_id, date, item) for item in single_advertiser_hour_data]
        advertiser_hour_report_update(total_data, advertiser_id, date)
    return single_advertiser_hour_data

def advertiser_id_date_append(advertiser_id, date, data):
    data['account_id'] = advertiser_id
    data['stat_date'] = date
    return data

def task_create(date):
    ins = tasks_log.insert()
    conn = engine_orm.connect()
    result = conn.execute(ins, platform_name='gdt', task_name = 'gdt_hour_advertiser_level_report', task_url = '/gdt/hour/advertiser/report/' + date, state = 0)
    task_id = result.inserted_primary_key[0]
    return task_id

def day_task_create(date):
    ins = tasks_log.insert()
    conn = engine_orm.connect()
    result = conn.execute(ins, platform_name='gdt', task_name = 'gdt_day_advertiser_level_report', task_url = '/gdt/day/advertiser/report/' + date, state = 0)
    task_id = result.inserted_primary_key[0]
    return task_id

def task_finish(task_id):
    stmt = tasks_log.update().where(tasks_log.c.id == task_id).values(end_time=datetime.datetime.now(), state = 1)
    conn = engine_orm.connect()
    conn.execute(stmt)

def advertiser_hour_report_update(total_data, advertiser_id, date):
    conn = engine_orm.connect()
    transaction = conn.begin()
    try:
        del_stmt = gdt_advertiser_hour_report_table.delete().where(gdt_advertiser_hour_report_table.c.account_id == advertiser_id).where(gdt_advertiser_hour_report_table.c.stat_date == date)
        insert_stmt = gdt_advertiser_hour_report_table.insert()
        conn.execute(del_stmt)
        conn.execute(insert_stmt, total_data)
        transaction.commit()
    except IntegrityError as error:
        transaction.rollback()
        logging.error(error)

@application.route('/gdt/day/advertiser/report/<string:date>')
async def gdt_advertiser_day_report(request, date):
    request.responseHeaders.addRawHeader(b"content-type", b"application/json")
    task_id = day_task_create(date)
    logging.info('gdt advertiser day report get start')
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    date = date if date else today
    token_rsp = await treq.get("http://social.emarbox.com/gdt/token/access")
    token = await token_rsp.content()
    logging.info("advertiser id total [%s]" % (gdt_advertiser_having_cost_set))
    advertiser_list = list(gdt_advertiser_having_cost_set)
    advertiser_list_chunks = [advertiser_list[x : x + 100] for x in range(0, len(advertiser_list), 100)]
    all_advertiser_day_data_total = []
    for advertiser_list_sub in advertiser_list_chunks:
        advertiser_day_report_total_deferred = [defer.ensureDeferred(single_advertiser_id_day_report_retrieve(advertiser_id, token, date)) for advertiser_id in advertiser_list_sub]
        advertiser_day_report_total_deferred_resolved = await defer.gatherResults(advertiser_day_report_total_deferred)
        advertiser_day_report_total_deferred_resolved_extract = [single_page for advertiser_single in advertiser_day_report_total_deferred_resolved if advertiser_single for single_page in advertiser_single]
        all_advertiser_day_data_total += advertiser_day_report_total_deferred_resolved_extract
        logging.info("gdt day report complete")
    task_finish(task_id)
    if all_advertiser_day_data_total:
        gdt_day_report_level_agent(all_advertiser_day_data_total, date)
    return json.dumps(all_advertiser_day_data_total)

def gdt_day_report_level_agent(gdt_ka_agent_day_report, date):
    engine_dsp = create_engine('mysql+pymysql://user:password@ip:port/dbname', pool_pre_ping=True, pool_recycle=36000)
    with engine_dsp.connect() as conn, conn.begin():
        gdt_day = pd.read_sql_query("SELECT * FROM gdt_daily_report_level_advertiser WHERE stat_date = '{}'".format(date), conn)
        gdt_day = gdt_day.loc[:, ['stat_date', 'view_count', 'download_count', 'activated_count', 'thousand_display_price', 'valid_click_count', 'cpc', 'cost', 'key_page_view_cost', 'platform_page_view_count', 'web_commodity_page_view_count', 'web_commodity_page_view_cost', 'web_register_count', 'page_consult_count', 'page_consult_cost', 'page_phone_call_direct_count', 'page_phone_call_direct_cost', 'page_phone_call_back_count', 'page_phone_call_back_cost', 'own_page_navigation_count', 'own_page_navi_cost', 'platform_page_navigation_count', 'platform_page_navigation_cost', 'platform_shop_navigation_count', 'platform_shop_navigation_cost', 'web_application_count', 'web_application_cost', 'page_reservation_count', 'page_reservation_cost', 'web_add_to_cart_count', 'web_add_to_cart_cost', 'add_to_cart_price', 'own_page_coupon_get_count', 'own_page_coupon_get_cost', 'platform_coupon_get_count', 'platform_coupon_get_cost', 'web_order_count', 'web_order_cost', 'web_checkout_amount', 'web_checkout_count', 'web_checkout_cost', 'order_amount', 'order_unit_price', 'deliver_count', 'deliver_cost', 'sign_in_count', 'sign_in_cost', 'download_cost', 'install_count', 'install_cost', 'activated_cost', 'retention_count', 'retention_cost', 'key_page_view_count', 'app_commodity_page_view_count', 'app_commodity_page_view_cost', 'app_register_count', 'app_register_cost', 'web_register_cost', 'app_application_count', 'app_application_cost', 'app_add_to_cart_count', 'add_to_cart_amount', 'app_add_to_cart_cost', 'app_order_count', 'app_order_cost', 'app_checkout_count', 'app_checkout_amount', 'app_checkout_cost', 'platform_coupon_click_count', 'follow_count', 'follow_cost', 'forward_count', 'forward_cost', 'read_count', 'read_cost', 'praise_count', 'praise_cost', 'comment_count', 'comment_cost', 'inte_phone_count', 'phone_call_count', 'external_form_reservation_count', 'ad_pur_val_web', 'ad_pur_val_app', 'game_create_role_count', 'game_authorize_count', 'game_tutorial_finish_count', 'effective_leads_count', 'effective_cost', 'effective_reserve_count', 'effective_consult_count', 'effective_phone_count', 'potential_reserve_count', 'potential_consult_count', 'potential_phone_count', 'arppu_cost', 'arpu_cost', 'cheout_fd', 'cheout_td', 'cheout_ow', 'cheout_tw', 'cheout_om', 'first_pay_count', 'pre_cre_web', 'pre_cre_app', 'pre_cre_web_val', 'pre_cre_app_val', 'cre_web', 'cre_app', 'cre_web_val', 'cre_app_val', 'withdr_dep_web', 'withdr_dep_app', 'withdr_dep_web_val', 'withdr_dep_app_val', 'first_pay_cost', 'landing_page_click_count', 'scan_follow_count', 'web_cart_amount', 'app_order_amount', 'web_order_amount', 'phone_consult_count', 'tool_consult_count', 'lottery_leads_count', 'lottery_leads_cost', 'conversions_count', 'conversions_cost', 'deep_conversions_count', 'deep_conversions_cost', 'first_memcard_web_count', 'first_memcard_app_count', 'memcard_web_count', 'memcard_app_count', 'first_memcard_web_cost', 'first_memcard_app_cost', 'valuable_click_count', 'valuable_click_cost', 'video_play_count', 'click_image_count', 'click_detail_count', 'click_head_count', 'click_nick_count', 'click_poi_count', 'video_inner_play_count', 'cpn_click_button_count', 'cpn_click_button_uv', 'key_page_uv', 'special_page_exp_uv', 'special_page_exp_cost', 'view_commodity_page_uv', 'overall_leads_purchase_count', 'leads_purchase_count', 'leads_purchase_cost', 'leads_purchase_uv', 'valid_leads_uv', 'phone_call_uv', 'valid_phone_uv', 'potential_customer_phone_uv', 'web_register_uv', 'web_apply_uv', 'web_credit_uv', 'app_apply_uv', 'app_pre_credit_uv', 'app_credit_uv', 'app_withdraw_uv', 'wechat_app_register_uv', 'no_interest_count', 'first_day_order_count', 'first_day_order_amount', 'add_wishlist_count', 'video_outer_play_count']]
        gdt_day_grouped = gdt_day.groupby(['stat_date'])
        gdt_day_grouped = gdt_day_grouped.agg({'view_count' : sum, 'download_count' : sum, 'activated_count' : sum, 'thousand_display_price' : np.mean, 'valid_click_count' : sum, 'cpc' : np.mean, 'cost' : sum, 'key_page_view_cost' : sum, 'platform_page_view_count' : sum, 'web_commodity_page_view_count' : sum, 'web_commodity_page_view_cost' : sum, 'web_register_count' : sum, 'page_consult_count' : sum, 'page_consult_cost' : sum, 'page_phone_call_direct_count' : sum, 'page_phone_call_direct_cost' : sum, 'page_phone_call_back_count' : sum, 'page_phone_call_back_cost' : sum, 'own_page_navigation_count' : sum, 'own_page_navi_cost' : sum, 'platform_page_navigation_count' : sum, 'platform_page_navigation_cost' : sum, 'platform_shop_navigation_count' : sum, 'platform_shop_navigation_cost' : sum, 'web_application_count' : sum, 'web_application_cost' : sum, 'page_reservation_count' : sum, 'page_reservation_cost' : sum, 'web_add_to_cart_count' : sum, 'web_add_to_cart_cost' : sum, 'add_to_cart_price' : sum, 'own_page_coupon_get_count' : sum, 'own_page_coupon_get_cost' : sum, 'platform_coupon_get_count' : sum, 'platform_coupon_get_cost' : sum, 'web_order_count' : sum, 'web_order_cost' : sum, 'web_checkout_amount' : sum, 'web_checkout_count' : sum, 'web_checkout_cost' : sum, 'order_amount' : sum, 'order_unit_price' : sum, 'deliver_count' : sum, 'deliver_cost' : sum, 'sign_in_count' : sum, 'sign_in_cost' : sum, 'download_cost' : sum, 'install_count' : sum, 'install_cost' : sum, 'activated_cost' : sum, 'retention_count' : sum, 'retention_cost' : sum, 'key_page_view_count' : sum, 'app_commodity_page_view_count' : sum, 'app_commodity_page_view_cost' : sum, 'app_register_count' : sum, 'app_register_cost' : sum, 'web_register_cost' : sum, 'app_application_count' : sum, 'app_application_cost' : sum, 'app_add_to_cart_count' : sum, 'add_to_cart_amount' : sum, 'app_add_to_cart_cost' : sum, 'app_order_count' : sum, 'app_order_cost' : sum, 'app_checkout_count' : sum, 'app_checkout_amount' : sum, 'app_checkout_cost' : sum, 'platform_coupon_click_count' : sum, 'follow_count' : sum, 'follow_cost' : sum, 'forward_count' : sum, 'forward_cost' : sum, 'read_count' : sum, 'read_cost' : sum, 'praise_count' : sum, 'praise_cost' : sum, 'comment_count' : sum, 'comment_cost' : sum, 'inte_phone_count' : sum, 'phone_call_count' : sum, 'external_form_reservation_count' : sum, 'ad_pur_val_web' : sum, 'ad_pur_val_app' : sum, 'game_create_role_count' : sum, 'game_authorize_count' : sum, 'game_tutorial_finish_count' : sum, 'effective_leads_count' : sum, 'effective_cost' : sum, 'effective_reserve_count' : sum, 'effective_consult_count' : sum, 'effective_phone_count' : sum, 'potential_reserve_count' : sum, 'potential_consult_count' : sum, 'potential_phone_count' : sum, 'arppu_cost' : sum, 'arpu_cost' : sum, 'cheout_fd' : sum, 'cheout_td' : sum, 'cheout_ow' : sum, 'cheout_tw' : sum, 'cheout_om' : sum, 'first_pay_count' : sum, 'pre_cre_web' : sum, 'pre_cre_app' : sum, 'pre_cre_web_val' : sum, 'pre_cre_app_val' : sum, 'cre_web' : sum, 'cre_app' : sum, 'cre_web_val' : sum, 'cre_app_val' : sum, 'withdr_dep_web' : sum, 'withdr_dep_app' : sum, 'withdr_dep_web_val' : sum, 'withdr_dep_app_val' : sum, 'first_pay_cost' : sum, 'landing_page_click_count' : sum, 'scan_follow_count' : sum, 'web_cart_amount' : sum, 'app_order_amount' : sum, 'web_order_amount' : sum, 'phone_consult_count' : sum, 'tool_consult_count' : sum, 'lottery_leads_count' : sum, 'lottery_leads_cost' : sum, 'conversions_count' : sum, 'conversions_cost' : sum, 'deep_conversions_count' : sum, 'deep_conversions_cost' : sum, 'first_memcard_web_count' : sum, 'first_memcard_app_count' : sum, 'memcard_web_count' : sum, 'memcard_app_count' : sum, 'first_memcard_web_cost' : sum, 'first_memcard_app_cost' : sum, 'valuable_click_count' : sum, 'valuable_click_cost' : sum, 'video_play_count' : sum, 'click_image_count' : sum, 'click_detail_count' : sum, 'click_head_count' : sum, 'click_nick_count' : sum, 'click_poi_count' : sum, 'video_inner_play_count' : sum, 'cpn_click_button_count' : sum, 'cpn_click_button_uv' : sum, 'key_page_uv' : sum, 'special_page_exp_uv' : sum, 'special_page_exp_cost' : sum, 'view_commodity_page_uv' : sum, 'overall_leads_purchase_count' : sum, 'leads_purchase_count' : sum, 'leads_purchase_cost' : sum, 'leads_purchase_uv' : sum, 'valid_leads_uv' : sum, 'phone_call_uv' : sum, 'valid_phone_uv' : sum, 'potential_customer_phone_uv' : sum, 'web_register_uv' : sum, 'web_apply_uv' : sum, 'web_credit_uv' : sum, 'app_apply_uv' : sum, 'app_pre_credit_uv' : sum, 'app_credit_uv' : sum, 'app_withdraw_uv' : sum, 'wechat_app_register_uv' : sum, 'no_interest_count' : sum, 'first_day_order_count' : sum, 'first_day_order_amount' : sum, 'add_wishlist_count' : sum, 'video_outer_play_count' : sum})
        gdt_day_grouped = gdt_day_grouped.reset_index()
        gdt_day_grouped['agent_id'] = 50567
        engine_dsp.execution_options(autocommit=True).execute("DELETE FROM gdt_daily_report_level_agent WHERE stat_date = '{}'".format(date))
        gdt_day_grouped[:].to_sql('gdt_daily_report_level_agent', engine_dsp.connect(), if_exists='append', method= 'multi', chunksize=2000, index = False)
        logging.info('gdt day report level agent completed')

async def single_page_gdt_advertiser_day_report(advertiser_id, page, token, date):
    fields_day_report = ['date','view_count','download_count','activated_count','activated_rate','thousand_display_price','valid_click_count','ctr','cpc','cost','key_page_view_cost','platform_page_view_count','platform_page_view_rate','web_commodity_page_view_count','web_commodity_page_view_cost','web_register_count','page_consult_count','page_consult_cost','page_phone_call_direct_count','page_phone_call_direct_cost','page_phone_call_back_count','page_phone_call_back_cost','own_page_navigation_count','own_page_navi_cost','platform_page_navigation_count','platform_page_navigation_cost','platform_shop_navigation_count','platform_shop_navigation_cost','web_application_count','web_application_cost','page_reservation_count','page_reservation_rate','page_reservation_cost','web_add_to_cart_count','web_add_to_cart_cost','add_to_cart_price','own_page_coupon_get_count','own_page_coupon_get_cost','platform_coupon_get_count','platform_coupon_get_cost','web_order_count','web_order_rate','app_order_rate','web_order_cost','web_checkout_amount','web_checkout_count','web_checkout_cost','order_amount','order_unit_price','order_roi','deliver_count','deliver_cost','sign_in_count','sign_in_cost','download_rate','download_cost','install_count','install_cost','click_activated_rate','activated_cost','retention_count','retention_rate','retention_cost','key_page_view_count','app_commodity_page_view_count','app_commodity_page_view_rate','web_commodity_page_view_rate','app_commodity_page_view_cost','app_register_count','app_register_cost','web_register_cost','app_application_count','app_application_cost','app_add_to_cart_count','add_to_cart_amount','app_add_to_cart_cost','app_order_count','app_order_cost','app_checkout_count','app_checkout_amount','app_checkout_cost','platform_coupon_click_count','platform_coupon_get_rate','follow_count','follow_cost','forward_count','forward_cost','read_count','read_cost','praise_count','praise_cost','comment_count','comment_cost','inte_phone_count','phone_call_count','external_form_reservation_count','ad_pur_val_web','ad_pur_val_app','game_create_role_count','game_authorize_count','game_tutorial_finish_count','effective_leads_count','effective_cost','effective_reserve_count','effective_consult_count','effective_phone_count','potential_reserve_count','potential_consult_count','potential_phone_count','app_checkout_rate','web_checkout_rate','app_activated_checkout_rate','web_activated_checkout_rate','app_register_rate','web_reg_rate','page_phone_call_direct_rate','page_phone_call_back_rate','page_consult_rate','deliver_rate','install_rate','arppu_cost','arpu_cost','cheout_fd','cheout_td','cheout_ow','cheout_tw','cheout_om','cheout_fd_reward','cheout_td_reward','cheout_ow_reward','cheout_tw_reward','cheout_om_reward','cheout_total_reward','first_pay_count','first_pay_rate','pre_cre_web','pre_cre_app','pre_cre_web_val','pre_cre_app_val','cre_web','cre_app','cre_web_val','cre_app_val','withdr_dep_web','withdr_dep_app','withdr_dep_web_val','withdr_dep_app_val','first_pay_cost','landing_page_click_count','scan_follow_count','web_cart_amount','app_order_amount','web_order_amount','phone_consult_count','tool_consult_count','lottery_leads_count','lottery_leads_cost','conversions_count','conversions_rate','conversions_cost','deep_conversions_count','deep_conversions_rate','deep_conversions_cost','first_memcard_web_count','first_memcard_app_count','memcard_web_count','memcard_app_count','first_memcard_web_rate','first_memcard_app_rate','first_memcard_web_cost','first_memcard_app_cost','valuable_click_count','valuable_click_rate','valuable_click_cost','video_play_count','click_image_count','click_detail_count','click_head_count','click_nick_count','click_poi_count','video_inner_play_count','cpn_click_button_count','cpn_click_button_uv','key_page_uv','special_page_exp_uv','special_page_exp_cost','view_commodity_page_uv','overall_leads_purchase_count','leads_purchase_count','leads_purchase_rate','leads_purchase_cost','leads_purchase_uv','valid_leads_uv','phone_call_uv','valid_phone_uv','potential_customer_phone_uv','web_register_uv','web_apply_uv','web_credit_uv','app_apply_uv','app_pre_credit_uv','app_credit_uv','app_withdraw_uv','wechat_app_register_uv','no_interest_count','first_day_order_count','first_day_order_amount','add_wishlist_count','video_outer_play_count']
    date_range={"start_date" : date, "end_date" : date}
    group_by = ["date"]
    order_by = [{"sort_field" : "date", "sort_type" : "ASCENDING"}]
    day_url = 'https://api.e.qq.com/v1.1/daily_reports/get?access_token=' + token.decode('utf-8') + '&timestamp=' + str(int(time.time())) + '&nonce=' + ran_str(31) + "&fields=" + json.dumps(fields_day_report) + "&page=" + str(page) + "&page_size=100" + "&account_id=" + str(advertiser_id) + "&level=REPORT_LEVEL_ADVERTISER" + "&date_range=" + json.dumps(date_range) + "&group_by=" + json.dumps(group_by) + "&order_by=" + json.dumps(order_by)
    day_res = await treq.get(day_url)
    day_data = await day_res.content()
    advertiser_day_data = json.loads(day_data.decode('utf-8'))
    if 'code' in advertiser_day_data and 'data' in advertiser_day_data and advertiser_day_data['code'] == 0 and advertiser_day_data['data']['list']:
        return advertiser_day_data
    else:
        logging.info("daily report data code [%s] message [%s] date [%s] advertiser id [%s] page [%s]" % (advertiser_day_data.get('code'), advertiser_day_data.get('message'), date, advertiser_id, page))
        return {}

async def single_advertiser_id_day_report_retrieve(advertiser_id, token, date):
    logging.info("advertiser id [%d] is processing" % (advertiser_id))
    single_advertiser_day_data = []
    single_gdt_advertiser_daily_report_single_page = await single_page_gdt_advertiser_day_report(advertiser_id, 1, token, date)
    if single_gdt_advertiser_daily_report_single_page and single_gdt_advertiser_daily_report_single_page['data']['list']:
        single_advertiser_day_data += single_gdt_advertiser_daily_report_single_page['data']['list']
        logging.info("advertiser id [%d] day report first page entered" % (advertiser_id))
    if single_gdt_advertiser_daily_report_single_page and single_gdt_advertiser_daily_report_single_page['data']['page_info']['total_page'] > 1:
        page_total = list(range(2, single_gdt_advertiser_daily_report_single_page['data']['page_info']['total_page'] + 1))
        logging.info("advertiser id [%s] day report second page entered" % (advertiser_id))
        advertiser_total_deferred = [defer.ensureDeferred(single_page_gdt_advertiser_day_report(advertiser_id, page, token, date)) for page in page_total]
        advertiser_total_deferred_resolved = await defer.gatherResults(advertiser_total_deferred)
        advertiser_total_deferred_resolved_extract = [single_page for advertiser_single in advertiser_total_deferred_resolved if advertiser_single and advertiser_single['data']['list'] for single_page in advertiser_single['data']['list']]
        single_advertiser_day_data += advertiser_total_deferred_resolved_extract
    if single_advertiser_day_data:
        total_data = [advertiser_id_date_append(advertiser_id, date, item) for item in single_advertiser_day_data]
        advertiser_day_report_update(total_data, advertiser_id, date)
    return single_advertiser_day_data

def advertiser_day_report_update(total_data, advertiser_id, date):
    conn = engine_orm.connect()
    transaction = conn.begin()
    try:
        del_stmt = gdt_advertiser_day_report_table.delete().where(gdt_advertiser_day_report_table.c.account_id == advertiser_id).where(gdt_advertiser_day_report_table.c.stat_date == date)
        insert_stmt = gdt_advertiser_day_report_table.insert()
        conn.execute(del_stmt)
        conn.execute(insert_stmt, total_data)
        transaction.commit()
    except IntegrityError as error:
        transaction.rollback()
        logging.error(error)

def batch_insert(sql, lines):
    try:
        engine = create_engine('mysql+pymysql://user:password@ip:port/dbname', pool_pre_ping=True, pool_recycle=36000)
        with engine.connect() as conn, conn.begin():
            for line in lines:
                conn.execution_options(autocommit=True).execute(text(sql), **line)
    except Exception as e:
        logging.error(e)

if __name__ == '__main__':
    application.run('0.0.0.0', 18075)
