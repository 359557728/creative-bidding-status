#!/usr/bin/python2.6
#-*- coding: UTF-8 -*-
# Copyright 2016 Emarbox Inc.
# Author: Chen Yunhai
# Create Time: 2016-06-28
import argparse
import urllib, urllib2
import httplib
import logging
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import re
import base64
import json
import kafka
import pymysql
import os
import time
from urlparse import urlparse 
import random
import hashlib

TOKEN = 'emarToken'
BID = 'b_teg_openrecom_ka7rvubxf17ekols8rfh'



CHANNEL_ID = 5120
NOW_DATE = ''
LOG_FILE_HANDLE = None
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Kafka config
KAFKA_INTERVAL = 10     # Unit: s
KAFKA_TOPIC = 'qualification'
KAFKA_GROUP_ID = 'consumer_5120'
KAFKA_IP_PORT_LIST = ['123.59.17.83:9092','123.59.17.180:9092','123.59.17.181:9092']
KAFKA_TIMEOUT = 1000    # Unit: ms
KAFKA_CAMPAIGN_SYNC_TOPIC = 'CampaignSync'
KAFKA_PARTITION_DICT = {}
KAFKA_PRODUCER = kafka.KafkaProducer(bootstrap_servers = KAFKA_IP_PORT_LIST)

# Api config
DSP_ID = '645426'
DSP_TOKEN = 'NTYyMzM4LDE0MzE5MzY5ODIsMGQzMWUyYTRjNmY1ZjEzZDdmY2ZmODVjYjU4YTQ0ZTY='
QUERY_CREATIVE_URL = 'https://api.e.qq.com/adx/v1/creative/get_review_status'
ADD_ADVERTISER_URL = 'https://api.e.qq.com/adx/v1/advertiser/add'
UPDATE_ADVERTISER_URL = 'https://api.e.qq.com/adx/v1/advertiser/update'
QUERY_ADVERTISER_URL = 'https://api.e.qq.com/adx/v1/advertiser/get_review_status'
ADD_CREATIVE_URL = 'https://api.e.qq.com/adx/v1/creative/add'
UPDATE_CREATIVE_URL = 'https://api.e.qq.com/adx/v1/creative/update'
QUERY_CREATIVE_INFO_URL = 'https://api.e.qq.com/adx/v1/creative/get'
QUERY_BID_URL = 'https://api.e.qq.com/adx/v1/report/bid'
QUERY_EXPOSURE_URL = 'https://api.e.qq.com/adx/v1/report/exposure'

ADD_ADVERTISER_MAX_SIZE = 5
UPDATE_ADVERTISER_MAX_SIZE = 5
QUERY_ADVERTISER_MAX_SIZE = 20
ADD_CREATIVE_MAX_SIZE = 10
UPDATE_CREATIVE_MAX_SIZE = 10
QUERY_CREATIVE_MAX_SIZE = 20

ADD_ADVERTISER_LIST = []
UPDATE_ADVERTISER_LIST = []
QUERY_ADVERTISER_LIST = []
ADD_CREATIVE_LIST = []
UPDATE_CREATIVE_LIST = []
QUERY_CREATIVE_LIST = []

# Mysql config
SQL_IP = '123.59.18.197'
SQL_PORT = 3406
SQL_USER = 'dsp'
SQL_PASSWORD = 'DSPprdDSP'
SQL_DB = 'dspprd'
SQL_CMD_LIST = []
SQL_ADVERTISER_TABLE = 'qualification_request'
SQL_CREATIVE_TABLE = 'creative_request'
SQL_APPROVED_ADVERTISER_TABLE = 'qualification_supplier'
SQL_CONSUMPTION_TABLE = 'channel_consumption'

def ChangeLoggingConfig() :
    global CHANNEL_ID
    global NOW_DATE
    global LOG_FILE_HANDLE
    now = int(time.time())
    time_array = time.localtime(now)
    styled_date_string = time.strftime("%Y-%m-%d", time_array)
    if not os.path.exists(str(CHANNEL_ID) + '-log') :
        try :
            os.makedirs(str(CHANNEL_ID) + '-log')
        except Exception as e :
            print e
            return
    if styled_date_string != NOW_DATE :
        NOW_DATE = styled_date_string
        file_handle = logging.FileHandler(str(CHANNEL_ID) + '-log/' + NOW_DATE, mode = 'a')
        formatter = logging.Formatter(fmt = '%(asctime)s %(filename)s line:%(lineno)d %(levelname)s %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')
        file_handle.setFormatter(formatter)
        if LOG_FILE_HANDLE != None :
            logger.removeHandler(LOG_FILE_HANDLE)
        LOG_FILE_HANDLE = file_handle
        logger.addHandler(LOG_FILE_HANDLE)

def WriteOffsetToFile() :
    global KAFKA_PARTITION_DICT
    if not os.path.exists('offset') :
        try :
            os.makedirs('offset')
        except Exception as e :
            logging.error(e)
            return
    offset_file = open('offset/Offset_' + str(CHANNEL_ID) + '.json', 'w')
    try :
        data = json.dumps(KAFKA_PARTITION_DICT)
        offset_file.write(data)
    except Exception as e :
        logging.error(e)
    finally :
        offset_file.close()   

def ReadOffsetFromFile(consumer) :
    global KAFKA_PARTITION_DICT
    if os.path.exists('offset/Offset_' + str(CHANNEL_ID) + '.json') :
        offset_file = open('offset/Offset_' + str(CHANNEL_ID) + '.json')
    else :
        return
    try :
        data = offset_file.read()
        try :
            KAFKA_PARTITION_DICT = json.loads(data)
        except Exception as e :
            logging.error(e)
            exit()
        if type(KAFKA_PARTITION_DICT) != dict :
            KAFKA_PARTITION_DICT = {}
            return
        partition_dict = {}
        for partition_id in KAFKA_PARTITION_DICT :
            partition_dict[kafka.structs.TopicPartition(topic = KAFKA_TOPIC, partition = int(partition_id))] = kafka.structs.OffsetAndMetadata(offset = KAFKA_PARTITION_DICT[partition_id], metadata = '')
        consumer.commit(offsets = partition_dict)
    except Exception as e :
        logging.error(e)
    finally :
        offset_file.close()
    
def SqlExecute() :
    global SQL_IP
    global SQL_PORT
    global SQL_USER
    global SQL_PASSWORD
    global SQL_DB
    global SQL_CMD_LIST
    logging.info('========SqlExecute=========')
    logging.info('SQL_CMD_LIST:'+str(SQL_CMD_LIST))
    if len(SQL_CMD_LIST) <= 0 :
        return
    try :
        conn = pymysql.connect(host = SQL_IP, port = SQL_PORT, user = SQL_USER, passwd = SQL_PASSWORD, db = SQL_DB, charset = 'utf8')
        cur = conn.cursor()
        for cmd in SQL_CMD_LIST :
            cur.execute(cmd);
        conn.commit()
        cur.close()
        conn.close()
        SQL_CMD_LIST = []
    except Exception as e :
        logging.error(e)

def SqlSelect(sql_cmd) :
    global SQL_IP
    global SQL_PORT
    global SQL_USER
    global SQL_PASSWORD
    global SQL_DB
    res = 'error'
    try :
        conn = pymysql.connect(host = SQL_IP, port = SQL_PORT, user = SQL_USER, passwd = SQL_PASSWORD, db = SQL_DB, charset = 'utf8')
        cur = conn.cursor()
        cur.execute(sql_cmd);
        res = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e :
        logging.error(e)
    return res
    
# End of tool functions

def SyncToCampaign(campaignId) :
    DSP_CAMP_SYNC_URL = 'http://api.displayer.emarbox.com/api/test/client/camaign/send?campaignId='
    url = DSP_CAMP_SYNC_URL + campaignId
    try :
        req = urllib2.Request(url)
        logging.info(url)
        handle = urllib2.urlopen(req)
        thepage = handle.read()
        logging.info(thepage)
    except IOError, e:
        logging.error(e)
        return e

def Post(url, json_string) :
    global DSP_TOKEN
    str_Authorization = 'Bearer ' + DSP_TOKEN
    headers = {
        "Content-Type": "application/json",
        "Authorization": str_Authorization
    }
    #print 'url:' + url + str(headers)
    try :
        #logging.info(str(url)+str(json_string)+str(headers))
        req = urllib2.Request(url,json_string,headers)
        handle = urllib2.urlopen(req)
        page = handle.read()
        #logging.info(page)
	logger.info('post_url:' + url + ' request_data:' + json_string + ' response_data:' + page)
        return page
    except Exception as e:
        print e
        logging.error(e)
        return 'error'
# End of tool functions

def addAdvertiser() :
    print '====================add_advertiser========================'
    global CHANNEL_ID
    global ADD_ADVERTISER_URL
    global ADD_ADVERTISER_LIST
    global ADD_ADVERTISER_MAX_SIZE
    global SQL_ADVERTISER_TABLE
    global SQL_CMD_LIST
    if len(ADD_ADVERTISER_LIST) == 0 :
        logging.debug('No advertiser to commit.')
        return
    cnt = 0
    commit_list = []
    now = int(time.time())
    time_array = time.localtime(now)
    styled_time_string = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    retry_advertiser_list = []
    retry_advertiser_tmp_list = []
    while len(ADD_ADVERTISER_LIST) > 0 :
        adv = ADD_ADVERTISER_LIST.pop()
        retry_advertiser_tmp_list.append(adv)
        if not adv.has_key('channel') and not type(adv['channel']) == int and CHANNEL_ID != creative['channel']:
            continue
        adv_dict = {}
        # Parse required string advertiser_id
        if not adv.has_key('project_id') or type(adv['project_id']) != int :
            logging.error('Project_id error.')
            continue
        adv_dict['advertiser_id'] = str(adv['project_id'])
        # Parse required string name
        if not adv.has_key('customer_name') or type(adv['customer_name']) != unicode :
            logging.error("Customer_name error.")
            continue
        adv_dict['name'] = adv['customer_name']
        # Parse required string homepage
        if adv.has_key('web_url') and type(adv['web_url']) == unicode :
            adv_dict['homepage'] = adv['web_url']
        else :
            logging.error("homepage error.")
            continue
        # Parse required int industry_id
        if not adv.has_key('channel_category') or type(adv['channel_category']) != list :
            logging.error("Channel_category error.")
            continue
        category_code = ''
        level = 0
        for cate in adv['channel_category'] :
            if type(cate) != dict or not cate.has_key('channel') or type(cate['channel']) != int or cate['channel'] != CHANNEL_ID or not cate.has_key('category') or type(cate['category']) != list or len(cate['category']) <= 0 :
                continue
            for code in cate['category'] :
                if type(code) != dict :
                    continue
                if code.has_key('level') and type(code['level']) == int :
                    if level >= code['level'] :
                        continue
                    else :
                        level = code['level']
                        if code.has_key('code') and type(code['code']) == unicode :
                            category_code = code['code']
                        break
        if category_code == '' :
            logging.error("Channel_category error too.")
            continue
        adv_dict['industry_id'] = category_code
        # Parse required array qualification_files
        qualifications = []
        if adv.has_key('images') and type(adv['images']) == list :
            for image in adv['images'] :
                if type(image) == dict and image.has_key('name') and type(image['name']) == unicode and image.has_key('url') and type(image['url']) == unicode :
                    AdvertiserQualification = {}
                    AdvertiserQualification['name'] = image['name']
                    AdvertiserQualification['file_url'] = image['url']
                    qualifications.append(AdvertiserQualification)
        if len(qualifications) > 0 :
            adv_dict['qualifications'] = qualifications
        else :
            logging.error('qualifications error.')
        # Parse optional string brand_name
        if adv.has_key('brand') and type(adv['brand']) == unicode :
            adv_dict['brand_name'] = adv['brand']
        # Parse optional string brand_logo_image_url
        # if adv.has_key('brand_logo_image_url') or type(adv['brand']) != unicode :
        # Parse optional long qq
        if adv.has_key('qq') and type(adv['qq']) == unicode :
            adv_dict['aa'] = long(adv['qq'])
        commit_list.append(adv_dict)
        cnt += 1
        if cnt == ADD_ADVERTISER_MAX_SIZE or (len(ADD_ADVERTISER_LIST) == 0 and cnt > 0) :
            commit_advertiser_dict = {}
            commit_advertiser_dict['data'] = commit_list
            json_string = json.dumps(commit_advertiser_dict)
            response_string = Post(ADD_ADVERTISER_URL, json_string)
            if response_string == 'error' :
                logging.error('Add advertiser post error.')
                #retry_advertiser_list.extend(retry_advertiser_tmp_list)
                continue
            try :
                response_json = json.loads(response_string, encoding = 'utf-8')
            except Exception as e :
                logging.error(e)
                continue
            print('response_json:'+str(response_json))
            commit_advertiser_dict = [];                                             
            if type(response_json) == dict and response_json.has_key('code') and response_json.has_key('message') :
                print 'ret message:' + response_json['message']                        
                for ret_data in response_json['data'] :
                    if not ret_data['code'] == 0 :
                        if ret_data['code'] == 10000 :
                            logging.error("gdt system error.")
                            #retry_advertiser_list.extend(retry_advertiser_tmp_list)
                        elif ret_data['code']  == 23002 :
                            sql_cmd = "UPDATE %s SET request_time = '%s', submit_status = 'submited', status = 'unaudited', request_result = '%s' WHERE project_id = %s AND supplier_id = %d" % (SQL_CREATIVE_TABLE, styled_time_string, ret_data['message'], ret_data['data']['advertiser_id'], CHANNEL_ID)
                            SQL_CMD_LIST.append(sql_cmd)
                        else :
                            sql_cmd = u"UPDATE %s SET response_time = '%s', status = 'rejected', submit_status = 'submited', request_result = '\u9519\u8bef\u7801: %d %s' WHERE supplier_id = %d AND project_id = %s" % (SQL_ADVERTISER_TABLE, styled_time_string, ret_data['code'], ret_data['message'], CHANNEL_ID, ret_data['data']['advertiser_id'])
                            SQL_CMD_LIST.append(sql_cmd)
                    else :
                        sql_cmd = "UPDATE %s SET request_time = '%s', status = 'unaudited', submit_status = 'submited' WHERE supplier_id = %d AND project_id = %s" % (SQL_CREATIVE_TABLE, styled_time_string, CHANNEL_ID, ret_data['data']['advertiser_id'])
                        SQL_CMD_LIST.append(sql_cmd)
                        logging.info("Commit advertiser success. advertiser_num = [%d]" % (cnt))
            else :
                print 'Add creative : Parse response error.'
                logging.error("Add advertiser : Parse response error." + str(response_string))
                continue
                #retry_advertiser_list.extend(retry_advertiser_tmp_list)
            #retry_advertiser_tmp_list = []
            cnt = 0
            commit_list = []
    ADD_ADVERTISER_LIST = []
    
def updateAdvertiser() :
    print '====================update_advertiser========================'
    global CHANNEL_ID
    global UPDATE_ADVERTISER_URL
    global UPDATE_ADVERTISER_MAX_SIZE
    global UPDATE_ADVERTISER_LIST
    global ADD_ADVERTISER_LIST
    global SQL_ADVERTISER_TABLE
    global SQL_CMD_LIST
    if len(UPDATE_ADVERTISER_LIST) == 0 :
        logging.debug('No advertiser to update.')
        return
    cnt = 0
    commit_list = []
    now = int(time.time())
    time_array = time.localtime(now)
    styled_time_string = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    retry_advertiser_list = []
    retry_advertiser_tmp_list = []
    while len(UPDATE_ADVERTISER_LIST) > 0 :
        adv = UPDATE_ADVERTISER_LIST.pop()
        retry_advertiser_tmp_list.append(adv)
        if not adv.has_key('channel') and not type(adv['channel']) == int and CHANNEL_ID != adv['channel'] :
            #print 'Channel error.'
            continue
        adv_dict = {}
        # Parse required string advertiser_id
        if not adv.has_key('project_id') or type(adv['project_id']) != int :
            logging.error("Project_id error.")
            continue
        adv_dict['advertiser_id'] = str(adv['project_id'])
        # Parse required string name
        if not adv.has_key('customer_name') or type(adv['customer_name']) != unicode :
            logging.error("Customer_name error.")
            continue
        adv_dict['name'] = adv['customer_name']
        # Parse required string homepage
        if adv.has_key('web_url') and type(adv['web_url']) == unicode :
            adv_dict['homepage'] = adv['web_url']
        else :
            logging.error("homepage error.")
            continue
        # Parse required int industry_id
        if not adv.has_key('channel_category') or type(adv['channel_category']) != list :
            print 'Channel_category error.'
            continue
        category_code = ''
        level = 0
        for cate in adv['channel_category'] :
            if type(cate) != dict or not cate.has_key('channel') or type(cate['channel']) != int or cate['channel'] != CHANNEL_ID or not cate.has_key('category') or type(cate['category']) != list or len(cate['category']) <= 0 :
                    continue
            for code in cate['category'] :
                if type(code) != dict :
                    continue
                if code.has_key('level') and type(code['level']) == int :
                    if level >= code['level'] :
                            continue
                    else :
                        level = code['level']
                        if code.has_key('code') and type(code['code']) == unicode :
                                category_code = code['code']
                        break
        if category_code == '' :
            logging.error("Channel_category error too.")
            continue
        adv_dict['industry_id'] = category_code
        # Parse required array qualification_files
        qualifications = []
        if adv.has_key('images') and type(adv['images']) == list :
            for image in adv['images'] :
                if type(image) == dict and image.has_key('name') and type(image['name']) == unicode and image.has_key('url') and type(image['url']) == unicode :
                    AdvertiserQualification = {}
                    AdvertiserQualification['name'] = image['name']
                    AdvertiserQualification['file_url'] = image['url']
                    qualifications.append(AdvertiserQualification)
        if len(qualifications) > 0 :
            adv_dict['qualifications'] = qualifications
        else :
            logging.error('qualifications error.')
        # Parse optional string brand_name
        if adv.has_key('brand') and type(adv['brand']) == unicode :
                adv_dict['brand_name'] = adv['brand']
        # Parse optional string brand_logo_image_url
        # if adv.has_key('brand_logo_image_url') or type(adv['brand']) != unicode :
        # Parse optional long qq
        if adv.has_key('qq') and type(adv['qq']) == unicode :
                adv_dict['aa'] = long(adv['qq'])
        commit_list.append(adv_dict)
        cnt += 1
        if cnt == UPDATE_ADVERTISER_MAX_SIZE or (len(UPDATE_ADVERTISER_LIST) == 0 and cnt > 0) :
            commit_advertiser_dict = {}
            commit_advertiser_dict['data'] = commit_list
            json_string = json.dumps(commit_advertiser_dict, encoding = 'utf-8')
            print('post the update data!')
            response_string = Post(UPDATE_ADVERTISER_URL, json_string)
            if response_string == 'error' :
                logging.error('Update advertiser post error.')
                #retry_advertiser_list.extend(retry_advertiser_tmp_list)
                continue
                #for adv in commit_list :
                #    sql_cmd = "UPDATE %s SET request_time = '%s', submit_status = 'submited' WHERE supplier_id = %d AND project_id = %s" % (SQL_ADVERTISER_TABLE, styled_time_string, CHANNEL_ID, adv['advertiser_id'])
                #    SQL_CMD_LIST.append(sql_cmd)
            try :
                response_json = json.loads(response_string, encoding = 'utf-8')
            except Exception as e :
                logging.error(e)
                #retry_advertiser_list.extend(retry_advertiser_tmp_list)
                continue
            print('response_json:'+str(response_json))
            commit_advertiser_dict = []
            if type(response_json) == dict and response_json.has_key('code') and response_json.has_key('message') :
                print 'ret message:' + response_json['message']
                for ret_data in response_json['data'] :
                    if not ret_data['code'] == 0 :
                        if ret_data['code'] == 10000 :
                            logging.error("gdt system error.")
                            #retry_advertiser_list.extend(retry_advertiser_tmp_list)
                        elif ret_data['code'] == 23001 :  # the advertiser was not find in server 广告主不存在
                            for re_adv in retry_advertiser_tmp_list :
                                if str(re_adv['advertiser_id']) == ret_data['data']['advertiser_id'] :
                                    #readd_creative_list.append(re_cr);
                                    logging.debug("advertiser readd:"+str(re_adv['advertiser_id']))
                                    print("advertiser readd:"+str(re_adv['advertiser_id']))
                                    ADD_ADVERTISER_LIST.append(re_adv);
                        else :
                            sql_cmd = "UPDATE %s SET response_time = '%s', status = 'rejected', submit_status = 'submited', request_result = '%s' WHERE supplier_id = %d AND project_id = %s" % (SQL_ADVERTISER_TABLE, styled_time_string, ret_data['message'], CHANNEL_ID, ret_data['data']['advertiser_id'])
                            SQL_CMD_LIST.append(sql_cmd)
                    else :
                        sql_cmd = "UPDATE %s SET request_time = '%s', status = 'unaudited', submit_status = 'submited' WHERE supplier_id = %d AND project_id = %s" % (SQL_CREATIVE_TABLE, styled_time_string, CHANNEL_ID, ret_data['data']['advertiser_id'])
                        SQL_CMD_LIST.append(sql_cmd)
                        logging.info("Commit advertiser success. advertiser_num = [%d]" % (cnt))
                        continue
            else :
                print 'Update advertiser : Parse response error.'
                logging.info("Update advertiser : Parse response error.")
                continue
            #retry_advertiser_tmp_list = []
            cnt = 0
            commit_list = []
    if len(ADD_ADVERTISER_LIST) > 0 :
        addAdvertiser()
    UPDATE_ADVERTISER_LIST = []

def queryAdvertiser() :
    print '====================query_advertiser========================'
    global CHANNEL_ID
    global QUERY_ADVERTISER_URL
    global QUERY_ADVERTISER_LIST
    global SQL_ADVERTISER_TABLE
    global SQL_CMD_LIST
    global QUERY_ADVERTISER_MAX_SIZE
    global SQL_APPROVED_ADVERTISER_TABLE
    #global KAFKA_IP_PORT_LIST
    #global KAFKA_CAMPAIGN_SYNC_TOPIC
    #global KAFKA_PRODUCER
    if len(QUERY_ADVERTISER_LIST) == 0 :
        logging.debug("No advertiser to query.")
        return
    query_advertiser_list = []
    #print('QUERY_ADVERTISER_LIST:'+str(QUERY_ADVERTISER_LIST))
    while len(QUERY_ADVERTISER_LIST) > 0 :
        adv = QUERY_ADVERTISER_LIST.pop()
        if not adv.has_key('channel') and not type(adv['channel']) == int and CHANNEL_ID != adv['channel'] :
            continue
        if adv.has_key('project_id') and type(adv['project_id']) == int :
            query_advertiser_list.append(str(adv['project_id']))
    if len(query_advertiser_list) > 0 :
        query_advertiser_dict = {}
        #query_advertiser_dict['dspid'] = DSP_ID
        #query_advertiser_dict['token'] = DSP_TOKEN
        query_advertiser_dict['data'] = query_advertiser_list
        json_string = json.dumps(query_advertiser_dict)
        print '==============json_strint=============='
        print json_string
        response_string = Post(QUERY_ADVERTISER_URL, json_string)
        if response_string == 'error' :
            logging.error('Query advertiser post error.')
        else :
            try :
                response_json = json.loads(response_string, encoding = 'utf-8')
            except Exception as e :
                logging.error(e)
                return
            if type(response_json) == dict and response_json.has_key('code') and type(response_json['code']) == int :
                print 'message' + response_json['message']
                now = int(time.time())
                time_array = time.localtime(now)
                styled_time_string = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
                for ret_data in response_json['data'] :
                    if not ret_data['code'] == 0 :
                        print 'query failed: code[%d],message[%s]' % (ret_data['code'], ret_data['message'])
                        continue
                    project_id = ret_data['data']['advertiser_id']
                    status_string = ''
                    submit_string = ''
                    reason = ''
                    if ret_data['data']['review_status'] == 'PENDING' :
                        status_string = 'unaudited' # Unaudited
                    elif ret_data['data']['review_status'] == 'APPROVED' :
                        status_string = 'approved'  # Approved
                        sql_cmd = "INSERT %s (project_id, supplier_id, update_time) VALUES (%s, %d, '%s') ON DUPLICATE KEY UPDATE update_time = '%s'" % (SQL_APPROVED_ADVERTISER_TABLE, project_id, CHANNEL_ID, styled_time_string, styled_time_string)
                        SQL_CMD_LIST.append(sql_cmd)
                    elif ret_data['data']['review_status'] == 'REJECTED' :
                        status_string = 'rejected'  # Rejected
                        submit_string = 'unsubmit'
                        if ret_data['data'].has_key('review_msg') and type(ret_data['data']['review_msg']) == unicode :
                            reason = ret_data['data']['review_msg']
                    sql_cmd = "UPDATE %s SET response_time = '%s', status = '%s'" % (SQL_ADVERTISER_TABLE, styled_time_string, status_string)
                    if submit_string != '' :
                        sql_cmd += ", submit_status = '%s'" % (submit_string)
                    if reason != '' :
                        sql_cmd += ", request_result = '%s'" % (reason.encode('utf-8'))
                    sql_cmd += " WHERE supplier_id = %d AND project_id = %s" % (CHANNEL_ID, project_id)
                    SQL_CMD_LIST.append(sql_cmd)
                # print 'sql cmd:'
                # print SQL_CMD_LIST
            else :
                print 'Query advertiser : Parse response error.'
    QUERY_ADVERTISER_LIST = []

def PostTenCentCloud(itemUpdateURL, json_string) :
    headers = {
        "Content-Type": "application/json"
        }
    try :
        req = urllib2.Request(itemUpdateURL,json_string,headers)
        handle = urllib2.urlopen(req)
        page = handle.read()
        return page
    except Exception as e:
        logging.error(e)
        return 'error'

def parseAndUpdate(cr_info) :
    time.sleep(0.01)
    global TOKEN
    creative_id = int(cr_info[0])
    creative_info = cr_info[1]
    if creative_info == None :
        logging.error('creative has no json [%s]'%(creative_id))
        return
    cr_json = json.loads(creative_info, encoding = 'utf-8')
    logging.info('creative_id:'+str(creative_id))
    #print('operate_type in json:'+str(cr_json['operate_type']))
    cr = {}
    if cr_json.has_key('native') :
        cr = cr_json['native'][0]	# a list
    else :
        logging.error('creative has no native key!')
        return
    now = time.time()
    now_sec = int(now)
    now = int(round(now*1000))
    commit_dict = {}
    request_id = str(now) + '_' + str(random.randint(100,1000))
    commit_dict['request_id'] = request_id
    commit_dict['data_type'] = 1
    commit_dict['bid'] = BID
    MD5 = BID + '&' + request_id + '&' + TOKEN
    m1 = hashlib.md5()
    m1.update(MD5)
    m1_get = m1.hexdigest()
    commit_dict['MD5'] = m1_get
    commit_dict['item_id'] = cr['creative_id']
    if cr.has_key('text') :
        for text_info in cr['text'] :
            if text_info.has_key('code') :
                if text_info['code'] == 'description' :
                    commit_dict['describe'] = text_info['content']
    if len(cr_info) >= 4 :
       commit_dict['tags'] = cr_info[3]	# cr_info[2] : code , cr_info[3] : code_text
    commit_dict['item_time'] = str(now_sec)
    commit_dict['expire_time'] = str(now_sec + 1 * 365 * 24 * 60 * 60)
    if cr.has_key('interative_type') :
        if cr['interative_type'] == 'android_download' :
            commit_dict['platform'] = 1
        if cr['interative_type'] == 'os_download' :
            commit_dict['platform'] = 2
    if cr.has_key('images') :
        for img_info in cr['images'] :
            if img_info.has_key('code') :
                if img_info['code'] == 'multimedia1_file_url' :
                    commit_dict['url'] = img_info['url']
    if cr.has_key('customer_name') :
        commit_dict['vender'] = cr['customer_name']
    json_string = json.dumps(commit_dict, encoding = 'utf-8')
    logging.info('json_string:'+str(json_string))
    #return
    itemUpdateURL = 'http://data.dm.qcloud.com:8088'
    response_string = PostTenCentCloud(itemUpdateURL, json_string)
    logging.info('response_string'+str(response_string))
    if response_string == 'error' :
        logging.error('Commit creative post error.')
        return
    try :
        response_json = json.loads(response_string, encoding = 'utf-8')
    except Exception as e :
        logging.error('json load error:'+str(e))
        return
    if response_json.has_key('code') :
        if response_json['code'] == -1 :
            logging.info('error [-1], format error.')
        if response_json['code'] == -2 :
            logging.info('error [-2], system error.')
        if response_json['code'] == -3 :
            logging.info('error [-3], algrithm error.')
        if response_json['code'] == 0 :
            logging.info('[%d]upload success!'%(creative_id))

def updateItemByCreative(creative_list) :
    cr_list = creative_list
    sql_cmd = 'select cr_req.creative_set_id,cr_req.request_json,cr_set.tag_code '\
            'from creative_request as cr_req, creative_set as cr_set '\
            'where cr_req.creative_set_id = cr_set.creative_set_id and cr_req.creative_set_id in ('
    sql_cmd += str(cr_list.pop())
    for cr_id in cr_list :
        sql_cmd += ','+str(cr_id)
    sql_cmd += ')'
    res = SqlSelect(sql_cmd)
    cr_info_list = []
    for row in res :
        cr_info = []
        for column in row :
            cr_info.append(column)
        if cr_info[2] <> None :
            tag_split = str(cr_info[2]).split(',')
            code_sql = 'select text_value from creative_tag where code in (\'%s\''%(str(tag_split.pop()))
            for tag in tag_split :
                code_sql += ',' + str(tag)
            code_sql += ')'
            code_res = SqlSelect(code_sql)
            str_code_list = ''
            code_cnt = 0
            for code_row in code_res :
                if code_cnt == 0 :
                    str_code_list += str(code_row[0])
                else :
                    str_code_list += ';' + str(code_row[0])
                code_cnt += 1
            cr_info.append(str_code_list)
        cr_info_list.append(cr_info)
    for cr in cr_info_list :
        parseAndUpdate(cr)
 
def addCreative() :
    print '====================addCreative========================'
    global CHANNEL_ID
    global ADD_CREATIVE_URL
    global ADD_CREATIVE_LIST
    global ADD_CREATIVE_MAX_SIZE
    global SQL_CMD_LIST
    global SQL_CREATIVE_TABLE
    if len(ADD_CREATIVE_LIST) == 0 :
        logging.info('No creative to commit.')
        return
    add_list = []
    #commit_app_list = []
    now = int(time.time())
    time_array = time.localtime(now)
    styled_time_string = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    cnt = 0
    retry_creative_list = []
    retry_creative_tmp_list = []
    while len(ADD_CREATIVE_LIST) > 0 :
        #print('has add_creative_list')
        logging.info("has add_creative_list.")
        creative = ADD_CREATIVE_LIST.pop()
        #print(creative)
        #print('channel:'+str(creative['channel']))
        retry_creative_tmp_list.append(creative)
        if not creative.has_key('creative_id') or type(creative['creative_id']) != int :
            logging.error("No creative id. [%s]" % (str(creative)))
            continue
        if not (creative.has_key('channel') and CHANNEL_ID == creative['channel']) :
            logging.error("No channel.")
            continue
        ###############
        #if not creative['creative_id'] == 747452 :
        #    continue
        ##########  
        logging.info('creative:'+str(creative['creative_id']))
        logging.info(creative)
        creative_dict = {}
        if creative[str(CHANNEL_ID) + '_ad_type'] == 'banner' :
            logging.error("creative tyoe is banner. Only allowed native!")
            continue    # native only
        # Parse required string
        if creative.has_key('project_id') :
            creative_dict['advertiser_id'] = str(creative['project_id'])
        else :
            logging.error("No project id. [%s]" % (str(creative)))
            continue
        creative_dict['creative_id'] = str(creative['creative_id'])
        is_dynamic = 0
        if creative.has_key('template_id') :
            creative_dict['creative_spec'] = creative['template_id']
            if creative_dict['creative_spec'] in [220, 222] :
                is_dynamic = 1
        else :
            logging.info("No template_id.")
            continue
        if not creative.has_key('monitor_url') and is_dynamic <> 1 :
            logging.error("No monitor_url.")
            continue
        target_url = ''
        monitor_url1 = ''
        for monitor_url in creative['monitor_url'] :
            if monitor_url.has_key('channel'):
              if monitor_url['channel'] == CHANNEL_ID and monitor_url.has_key('click_url') and monitor_url.has_key('impression_url') :
                  target_url = monitor_url['click_url']
                  monitor_url1 = monitor_url['impression_url']
        click_monitor = ''
        mm_monitor = ''
        if not target_url == '' and is_dynamic <> 1 :
            #print('target_url')
            click_monitor = target_url.replace("info=%%CLICK_PARAM%%&","")
            click_monitor += "&bid_request_id=__BID_REQUEST_ID__"
            click_monitor = click_monitor.replace("mm.emarbox.com","sm.emarbox.com")
            mm_monitor = target_url[0:target_url.find("?")]
            mm_monitor += "?info=__CLICK_PARAM__"
            creative_dict['click_monitor_url1'] = mm_monitor
            #creative_dict['target_url'] = target_url
            #click_monitor = target_url.replace("http://mm.emarbox.com/","http://sm.emarbox.com/")
            #if target_url.find("http://") <> -1 :
            #    creative_dict['click_monitor_url1'] = "http://sm.emarbox.com/tencentGDT/click?info=%%CLICK_PARAM%%"
            #if target_url.find("https://") <> -1 :
            #    creative_dict['click_monitor_url1'] = "https://sm.emarbox.com/tencentGDT/click?info=%%CLICK_PARAM%%"
        elif is_dynamic <> 1 :
            logging.error("No target_url.")
            continue
        if not monitor_url1 == '' and is_dynamic <> 1 :
            #creative_dict['monitor_url1'] = monitor_url1
            adx_monitor_url1 = monitor_url1.replace("%%IMPRESSION_PARAM%%","__IMPRESSION_PARAM__")
            adx_monitor_url1 = adx_monitor_url1.replace("%%WIN_PRICE%%","__WIN_PRICE__")
            creative_dict['monitor_url1'] = adx_monitor_url1
        elif is_dynamic <> 1 :
            logging.error("No monitor_url1.")
            continue
        #if creative.has_key('landingpage_url') :
        #    creative_dict['landing_page'] = creative['landingpage_url']
        #if creative[str(CHANNEL_ID) + '_ad_type'] == 'native' :    # native creative
        monitor_url2 = ''
        #if creative.has_key('third_monitor_url') :
            #if creative['third_monitor_url'].has_key('impression_url') :
                #monitor_url2 = creative['third_monitor_url']['impression_url']
        if not monitor_url2 == '' :
            creative_dict['monitor_url2'] = monitor_url2
        #if creative.has_key('template_id') :
        #    creative_dict['creative_spec'] = creative['template_id']
        #else :
        #    logging.info("No template_id.")
        #    continue
        creative_dict['product_type'] = 1    # default
        if creative.has_key('interative_type') :
            if creative['interative_type'] == 'browser' :
                creative_dict['product_type'] = 1
            if creative['interative_type'] == 'android_download' :
                creative_dict['product_type'] = 2
                #creative_dict['click_monitor_url1'] = target_url
            if creative['interative_type'] == 'os_download' :
                creative_dict['product_type'] = 3
                #creative_dict['click_monitor_url1'] = target_url
        else :
            logging.info("No interative_type.")
	if not creative_dict.has_key('product_tpye') :
	    creative_dict['product_type'] = 1
        if not click_monitor == '' and creative.has_key('landingpage_url') and not (creative_dict['product_type'] == 2 or creative_dict['product_type'] == 3):
            #creative_dict['landing_page'] = creative['landingpage_url']
            creative_dict['click_through_url'] = click_monitor
        if creative_dict['product_type'] == 2 or creative_dict['product_type'] == 3 :
            click_monitor_url1 = target_url.replace("info=%%CLICK_PARAM%%&","")
            click_monitor_url1 += "&info=__CLICK_PARAM__"
            creative_dict['click_monitor_url1'] = click_monitor_url1
        # Parse potional string
        if creative.has_key('text') :
            for text_info in creative['text'] :
                if text_info.has_key('code') :
                    if text_info['code'] == 'title' and  text_info.has_key('content') :
                        #if (not creative_dict['product_type'] in [2,3]) or  creative_dict['creative_spec'] == 65 :
                        creative_dict['title'] = text_info['content']
                    if text_info['code'] == 'description' :
                        if text_info.has_key('content') :
                            creative_dict['description'] = text_info['content']
                    if text_info['code'] == 'call_to_action' :
                        if text_info.has_key('content') and not (creative_dict['creative_spec'] in [184,210,65]) :
                            creative_dict['call_to_action'] = text_info['content']
                    if text_info['code'] == 'deeplink_url' :
                        if text_info.has_key('content') and not (creative_dict['creative_spec'] in [184,210,65]) :
                            creative_dict['deep_link_uri'] = text_info['content']
                    if creative_dict.has_key('product_type') and creative_dict['product_type'] == 2 :
                        if text_info['code'] == 'android_appid' :
                            creative_dict['app_id'] = long(text_info['content'])
                    if creative_dict.has_key('product_type') and creative_dict['product_type'] in [2, 3] :
                        if text_info['code'] == 'appid' :
                            creative_dict['app_id'] = long(text_info['content'])
                    if (text_info['code'] == 'android_apk_name' or text_info['code'] == 'package_name') and creative_dict['product_type'] == 2 :
                        if text_info.has_key('content') :
                            creative_dict['pkgname'] = text_info['content']
                    if text_info['code'] == 'brand name' :
                        if text_info.has_key('content') and creative_dict['creative_spec'] == 65 :
                            creative_dict['description'] = text_info['content']
        else :
            logging.info("No text.")
        creative_dict['multimedia1_file_url'] = ''
        creative_dict['multimedia2_file_url'] = ''
        creative_dict['multimedia3_file_url'] = ''
        if creative.has_key('images') :
            for img_info in creative['images'] :
                if img_info.has_key('code') :
                    if img_info['code'] == 'multimedia1_file_url' :
                        creative_dict['multimedia1_file_url'] = img_info['url']
                    if img_info['code'] == 'multimedia2_file_url' and creative_dict['product_type'] == 1 :
                        creative_dict['multimedia2_file_url'] = img_info['url']
                    if img_info['code'] == 'brand_logo_image_url' and creative_dict['creative_spec'] in [65] :
                        creative_dict['multimedia2_file_url'] = img_info['url']
                    if img_info['code'] == 'multimedia3_file_url' and creative_dict['creative_spec'] in [185] :
                        creative_dict['multimedia3_file_url'] = img_info['url']
        else :
            logging.info("No images.")
        endtime = now + 178 * 24 * 60 * 60
        time_array2 = time.localtime(endtime)
        styled_time_string_end = time.strftime("%Y-%m-%d", time_array2)
        creative_dict['end_date_included'] = styled_time_string_end
        add_list.append(creative_dict)
        cnt += 1
        logging.info('cnt:'+str(cnt)+',len_add_list:'+str(len(ADD_CREATIVE_LIST)))
        if cnt == ADD_CREATIVE_MAX_SIZE or (cnt > 0 and len(ADD_CREATIVE_LIST) == 0) :
            commit_creative_dict = {}
            commit_creative_dict['data'] = add_list
            logging.info('post the add date!'+str(creative['creative_id']))
            json_string = json.dumps(commit_creative_dict, encoding = 'utf-8')
            logging.info(json_string)
            response_string = Post(ADD_CREATIVE_URL, json_string)
            if response_string == 'error' :
                logging.error('Commit creative post error.')
                #retry_creative_list.extend(retry_creative_tmp_list)
                #retry_creative_tmp_list = []
                continue
            try :
                response_json = json.loads(response_string, encoding = 'utf-8')
            except Exception as e :
                logging.error(e)
                #retry_creative_list.extend(retry_creative_tmp_list)
                #retry_creative_tmp_list = []
                continue
            logging.info('response_json:'+str(response_json))
            if type(response_json) != dict :
                logging.error("Response error. [%s]" % (response_string))
                #retry_creative_list.extend(retry_creative_tmp_list)
                #retry_creative_tmp_list = []
                continue
            #for creative in add_list :
            #    sql_cmd = "UPDATE %s SET request_time = '%s', status = 'unaudited', submit_status = 'submited' WHERE supplier_id = %d AND creative_set_id = %s" % (SQL_CREATIVE_TABLE, styled_time_string, CHANNEL_ID, creative['creative_id'])
            #    SQL_CMD_LIST.append(sql_cmd)
            commit_creative_dict = [];
            if type(response_json) == dict and response_json.has_key('code') and response_json.has_key('message') :
                #print 'ret message:' + response_json['message']
                for ret_data in response_json['data'] :
                    if not ret_data['code'] == 0 :
                        if ret_data['code'] == 10000 :
                            logging.error("gdt system error.")
                            #retry_creative_list.extend(retry_creative_tmp_list)
                            #retry_creative_tmp_list = []
                        elif ret_data['code']  == 26002 :
                           #sql_cmd = "UPDATE %s SET request_time = '%s', submit_status = 'submited', status = 'unaudited', request_result = '%s' WHERE creative_set_id = %s AND supplier_id = %d" % (SQL_CREATIVE_TABLE, styled_time_string, ret_data['message'], ret_data['data']['creative_id'], CHANNEL_ID)
                           #SQL_CMD_LIST.append(sql_cmd)
                           continue
                        else :
                            str_message = str(ret_data['message']).replace('\'',' ')
                            sql_cmd = u"UPDATE %s SET request_time = '%s', submit_status = 'submited', status = 'rejected', request_result = '\u9519\u8bef\u7801: %d %s' WHERE creative_set_id = %s AND supplier_id = %d" % (SQL_CREATIVE_TABLE, styled_time_string, ret_data['code'], str_message, ret_data['data']['creative_id'], CHANNEL_ID)
                            SQL_CMD_LIST.append(sql_cmd)
                    else :
                        sql_cmd = "UPDATE %s SET request_time = '%s', status = 'unaudited', submit_status = 'submited', request_result = '' WHERE supplier_id = %d AND creative_set_id = %s" % (SQL_CREATIVE_TABLE, styled_time_string, CHANNEL_ID, ret_data['data']['creative_id'])
                        SQL_CMD_LIST.append(sql_cmd)
                        #retry_creative_tmp_list = []
                        continue
                    #retry_creative_tmp_list = []
            else :
                #print 'Add creative : Parse response error.'
                logging.error("Add creative : Parse response error. [%s]" % (response_string))
                #retry_creative_list.extend(retry_creative_tmp_list)
                #retry_creative_tmp_list = []
                continue
            cnt = 0
            add_list = []
    ADD_CREATIVE_LIST = []
        
def updateCreative() :
    print '====================updateCreative========================'
    global UPDATE_CREATIVE_URL
    global CHANNEL_ID
    global UPDATE_CREATIVE_LIST
    global ADD_CREATIVE_LIST
    global UPDATE_CREATIVE_MAX_SIZE
    global SQL_CMD_LIST
    global SQL_CREATIVE_TABLE
    if len(UPDATE_CREATIVE_LIST) == 0 :
        logging.info('No creative to commit.')
        return
    update_list = []
    # for creative in UPDATE_CREATIVE_LIST :
    now = int(time.time())
    time_array = time.localtime(now)
    styled_time_string = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    cnt = 0
    retry_creative_list = []
    retry_creative_tmp_list = []
    while len(UPDATE_CREATIVE_LIST) > 0 :
        creative = UPDATE_CREATIVE_LIST.pop()
        retry_creative_tmp_list.append(creative)
        if not creative.has_key('creative_id') or type(creative['creative_id']) != int :
            logging.error("No creative id. [%s]" % (str(creative)))
            continue
        if not (creative.has_key('channel') and CHANNEL_ID == creative['channel']) :
            logging.error("No channel.")
            continue
        creative_dict = {}
        if creative[str(CHANNEL_ID) + '_ad_type'] == 'banner' :
            continue    # native only
        # Parse required string
        if creative.has_key('project_id') :
            creative_dict['advertiser_id'] = str(creative['project_id'])
        else :
            logging.error("No project id. [%s]" % (str(creative)))
            continue
        creative_dict['creative_id'] = str(creative['creative_id'])
        if not creative.has_key('monitor_url') :
            logging.error("No monitor_url.")
            continue
        target_url = ''
        monitor_url1 = ''
        for monitor_url in creative['monitor_url'] :
            if monitor_url.has_key('channel'):
              if monitor_url['channel'] == CHANNEL_ID :
                  target_url = monitor_url['click_url']
                  monitor_url1 = monitor_url['impression_url']
        click_monitor = ''
        mm_monitor = ''
        if not target_url == '' :
            #creative_dict['target_url'] = target_url
            click_monitor = target_url.replace("info=%%CLICK_PARAM%%&","")
            click_monitor += "&bid_request_id=__BID_REQUEST_ID__"
            click_monitor = click_monitor.replace("mm.emarbox.com","sm.emarbox.com")
            mm_monitor = target_url[0:target_url.find("?")]
            mm_monitor += "?info=__CLICK_PARAM__"
            creative_dict['click_monitor_url1'] = mm_monitor
            #click_monitor = target_url.replace("http://mm.emarbox.com/","http://sm.emarbox.com/")
            #if target_url.find("http://") <> -1 :
            #    creative_dict['click_monitor_url1'] = "http://sm.emarbox.com/tencentGDT/click?info=%%CLICK_PARAM%%"
            #elif target_url.find("https://") <> -1 :
            #    creative_dict['click_monitor_url1'] = "https://sm.emarbox.com/tencentGDT/click?info=%%CLICK_PARAM%%"
            #logger.info(target_url)
        else :
            logging.error("No target_url.")
            continue
        if not monitor_url1 == '' :
            #creative_dict['monitor_url1'] = monitor_url1
            adx_monitor_url1 = monitor_url1.replace("%%IMPRESSION_PARAM%%","__IMPRESSION_PARAM__")
            adx_monitor_url1 = adx_monitor_url1.replace("%%WIN_PRICE%%","__WIN_PRICE__")
            creative_dict['monitor_url1'] = adx_monitor_url1
        else :
            logging.error("No monitor_url1.")
            continue
        if creative.has_key('template_id') :
            creative_dict['creative_spec'] = creative['template_id']
        else :
            logging.error("No template_id.")
            continue
        if creative.has_key('interative_type') :
            if creative['interative_type'] == 'browser' :
                creative_dict['product_type'] = 1
            if creative['interative_type'] == 'android_download' :
                creative_dict['product_type'] = 2
                #creative_dict['click_monitor_url1'] = target_url
            if creative['interative_type'] == 'os_download' :
                creative_dict['product_type'] = 3
                #creative_dict['click_monitor_url1'] = target_url
        else :
            logging.info("No interative_type.")
	if not creative_dict.has_key('product_type') :
	    creative_dict['product_type'] = 1
        if not target_url == '' and creative.has_key('landingpage_url') and not (creative_dict['product_type'] == 2 or creative_dict['product_type'] == 3):
            #creative_dict['landing_page'] = creative['landingpage_url']
            creative_dict['click_through_url'] = click_monitor
            #creative_dict['click_through_url'] = target_url
        if creative_dict['product_type'] == 2 or creative_dict['product_type'] == 3 :
            click_monitor_url1 = target_url.replace("info=%%CLICK_PARAM%%&","")
            click_monitor_url1 += "&info=__CLICK_PARAM__"
            creative_dict['click_monitor_url1'] = click_monitor_url1
        if creative.has_key('text') :
            for text_info in creative['text'] :
                if text_info.has_key('code') :
                    if text_info['code'] == 'title'  and  text_info.has_key('content') :
                        #if (not creative_dict['product_type'] in [2,3]) or creative_dict['creative_spec'] == 65 :
                        creative_dict['title'] = text_info['content']
                    if text_info['code'] == 'description' :
                        if text_info.has_key('content') :
                            creative_dict['description'] = text_info['content']
                    if text_info['code'] == 'call_to_action' :
                        if text_info.has_key('content')  and not (creative_dict['creative_spec'] in [184,210,65]) :
                            creative_dict['call_to_action'] = text_info['content']
                    if text_info['code'] == 'deeplink_url' :
                        if text_info.has_key('content')  and not (creative_dict['creative_spec'] in [184,210,65]) :
                            creative_dict['deep_link_uri'] = text_info['content']
                    if creative_dict.has_key('product_type') and creative_dict['product_type'] == 2 :
                        if text_info['code'] == 'android_appid' :
                            creative_dict['app_id'] = long(text_info['content'])
                    if creative_dict.has_key('product_type') and creative_dict['product_type'] in [2, 3] :
                        if text_info['code'] == 'appid' :
                            creative_dict['app_id'] = text_info['content']
                    if (text_info['code'] == 'android_apk_name' or text_info['code'] == 'package_name')and creative_dict['product_type'] == 2 :
                        if text_info.has_key('content') :
                            creative_dict['pkgname'] = text_info['content']
        else :
            logging.info("No text.")
        creative_dict['multimedia1_file_url'] = ''
        creative_dict['multimedia2_file_url'] = ''
        if creative.has_key('images') :
            for img_info in creative['images'] :
                if img_info.has_key('code') :
                    if img_info['code'] == 'multimedia1_file_url' :
                        creative_dict['multimedia1_file_url'] = img_info['url']
                    if img_info['code'] == 'multimedia2_file_url' and creative_dict['product_type'] == 1 :
                        creative_dict['multimedia2_file_url'] = img_info['url']
                    if img_info['code'] == 'brand_logo_image_url' and creative_dict['creative_spec'] == 65 :
                        creative_dict['multimedia2_file_url'] = img_info['url']
                    if img_info['code'] == 'multimedia3_file_url' and creative_dict['creative_spec'] in [185] :
                        creative_dict['multimedia3_file_url'] = img_info['url']
        else :
            logging.info("No images.")
        endtime = now + 178 * 24 * 60 * 60 
        time_array2 = time.localtime(endtime) 
        styled_time_string_end = time.strftime("%Y-%m-%d", time_array2)
        creative_dict['end_date_included'] = styled_time_string_end
        update_list.append(creative_dict)
        cnt += 1
        print('cnt:'+str(cnt)+'len_add_list:'+str(len(UPDATE_CREATIVE_LIST)))
        if cnt == UPDATE_CREATIVE_MAX_SIZE or (cnt > 0 and len(UPDATE_CREATIVE_LIST)) == 0 :
            commit_creative_dict = {}
            commit_creative_dict['data'] = update_list
            json_string = json.dumps(commit_creative_dict, encoding = 'utf-8')
            print('post the update data!')
            print(json_string)
            response_string = Post(UPDATE_CREATIVE_URL, json_string)
            if response_string == 'error' :
                logging.error('Commit creative post error.')
                #retry_creative_list.extend(retry_creative_tmp_list)
                #retry_creative_tmp_list = []
                continue
            try :
                response_json = json.loads(response_string, encoding = 'utf-8')
            except Exception as e :
                logging.error(e)
                #retry_creative_list.extend(retry_creative_tmp_list)
                #retry_creative_tmp_list = []
                continue
            logging.info('response_json:'+str(response_json))
            if type(response_json) != dict :
                logging.error("Response error. [%s]" % (response_string))
                #retry_creative_list.extend(retry_creative_tmp_list)
                #retry_creative_tmp_list = []
                continue
            #for creative in commit_creative_dict :
            #    sql_cmd = "UPDATE %s SET request_time = '%s', status = 'unaudited', submit_status = 'submited' WHERE supplier_id = %d AND creative_set_id = %s" % (SQL_CREATIVE_TABLE, styled_time_string, CHANNEL_ID, creative['creative_id'])
            #    SQL_CMD_LIST.append(sql_cmd)
            commit_creative_dict = [];
            if type(response_json) == dict and response_json.has_key('code') and response_json.has_key('message') :
                #print 'ret message:' + response_json['message']
                readd_creative_list = []
                for ret_data in response_json['data'] :
                    if not ret_data['code'] == 0 :
                        if ret_data['code'] == 10000 :
                            logging.error("gdt system error.")
                            #retry_advertiser_list.extend(retry_creative_tmp_list)
                            #retry_creative_tmp_list = []
                        #elif ret_data['code'] == 26044 :
                        #    logging.error("error:creative_id:[%s],[%s]" %(ret_data['data']['creative_id'],ret_data['message']))
                        #    re_query_dict = {}
                        #    re_query_dict['creative_id'] = int(ret_data['data']['creative_id'])
                        #    re_query_dict['channel'] = CHANNEL_ID
                        #    re_query_dict['project_id'] = int(ret_data['data']['advertiser_id'])
                        #    QUERY_CREATIVE_LIST.append(re_query_dict)
                        elif ret_data['code'] == 26001 :  # the creative was not find in server 广告不存在
                            for re_cr in retry_creative_tmp_list :
                                if str(re_cr['creative_id']) == ret_data['data']['creative_id'] :
                                    #readd_creative_list.append(re_cr);
                                    logging.debug("creative readd:"+str(re_cr['creative_id']))
                                    print("creative readd:"+str(re_cr['creative_id']))
                                    ADD_CREATIVE_LIST.append(re_cr);
                        else :
                            str_message = str(ret_data['message'].replace('\'',' '))
                            sql_cmd = "UPDATE %s SET response_time = '%s', submit_status = 'submited', status = 'rejected', request_result = '\u9519\u8bef\u7801: %d %s' WHERE creative_set_id = %s AND supplier_id = %s" % (SQL_CREATIVE_TABLE, styled_time_string, ret_data['code'], str_message, ret_data['data']['creative_id'], CHANNEL_ID)
                            SQL_CMD_LIST.append(sql_cmd)
                    else :
                        sql_cmd = "UPDATE %s SET request_time = '%s', status = 'unaudited', submit_status = 'submited', request_result = '' WHERE supplier_id = %d AND creative_set_id = %s" % (SQL_CREATIVE_TABLE, styled_time_string, CHANNEL_ID, ret_data['data']['creative_id'])
                        SQL_CMD_LIST.append(sql_cmd)
                        #retry_creative_tmp_list = []
                        continue
                    #retry_creative_tmp_list = []
            else :
                print 'Update creative : Parse response error.'
                logging.error("Update creative : Parse response error. [%s]" % (response_string))
                #retry_creative_list.extend(retry_creative_tmp_list)
                #retry_creative_tmp_list = []
                continue
            cnt = 0
            update_list = []
    if len(ADD_CREATIVE_LIST) > 0 :
        addCreative()
    UPDATE_CREATIVE_LIST = []

def queryCreatvieInfo(creatives) :
    print "====================queryCreatvieInfo========================"
    if not type(creatives) == list :
        print 'creatives type wrong'+str(creatives)
        return
    cr_info_list = []
    for cr in creatives :
        project_id = 0
        sql_cmd = 'select project_id from creative where id = (select creative_id from creative_set where creative_set_id=%s)' % str(cr)
        res = SqlSelect(sql_cmd)
        if res == 'error' :
            print 'query project id error'
            return
        for result in res :
            for pro_id in result :
                project_id = pro_id
        query_cr_info_dict = {}
        query_cr_info_dict['advertiser_id'] = str(project_id)
        query_cr_info_dict['creative_id'] = str(cr)
        cr_info_list.append(query_cr_info_dict)
    print cr_info_list
    if not len(cr_info_list) > 0 :
        print 'nothing in cr_info_list:' + str(cr_info_list)
        return
    query_creative_info_dict = {}
    query_creative_info_dict['data'] = cr_info_list
    json_string = json.dumps(query_creative_info_dict, encoding = 'utf-8')
    response_string = Post(QUERY_CREATIVE_INFO_URL, json_string)
    if response_string == 'error' :
            print('Post error')
            return
    try :
        response_json = json.loads(response_string, encoding = 'utf-8')
    except Exception as e :
        print(e)
        return
    print('response_json:'+str(response_json))

def queryCreative(print_result = False) :
    logging.info('====================queryCreative========================')
    global CHANNEL_ID
    global QUERY_CREATIVE_URL
    global QUERY_CREATIVE_LIST
    global SQL_CREATIVE_TABLE
    global SQL_CMD_LIST
    global QUERY_CREATIVE_MAX_SIZE
    global KAFKA_IP_PORT_LIST
    global KAFKA_CAMPAIGN_SYNC_TOPIC
    global KAFKA_PRODUCER
    if len(QUERY_CREATIVE_LIST) == 0 :
        logging.info("No creative to query.")
        return
    query_creative_list = []
    print_list = []
    logging.info(QUERY_CREATIVE_LIST)
    while len(QUERY_CREATIVE_LIST) > 0 :
        creative = QUERY_CREATIVE_LIST.pop()
        if not creative.has_key('creative_id') or type(creative['creative_id']) != int :
            logging.error("No creative_id. [%s]" % (str(creative)))
            continue        
        query_cr_dict = {}
        if not creative.has_key('project_id') :
            logging.error('no project id!')
            continue
        #print('creative:'+str(creative))
        query_cr_dict['advertiser_id'] = str(creative['project_id'])
        query_cr_dict['creative_id'] = str(creative['creative_id'])
        query_creative_list.append(query_cr_dict)
    if len(query_creative_list) == 0 :
        return
    # query_creative_list = cr_id_list
    if len(query_creative_list) > 0 :
        query_creative_dict = {}
        query_creative_dict['data'] = query_creative_list
        json_string = json.dumps(query_creative_dict, encoding = 'utf-8')
        logging.info('query_creative_dict:'+str(json_string))
        response_string = Post(QUERY_CREATIVE_URL, json_string)
        if response_string == 'error' :
            logging.error('Post error')
            return
        try :
            response_json = json.loads(response_string, encoding = 'utf-8')
        except Exception as e :
            logging.error(e)
            return
        # print 'query creative:'
        # print response_json
        logging.info('response_json:'+str(response_json))
        update_item_list = []
        if type(response_json) == dict and response_json.has_key('code') and type(response_json['code']) :
            #print 'message:' + response_json['message']
            now = int(time.time())
            time_array = time.localtime(now)
            styled_time_string = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
            update_kafka_list = []
            for ret_data in response_json['data'] :
                if not ret_data['code'] == 0 :
                    print 'query failed: code[%d],message[%s]' % (ret_data['code'], ret_data['message'])
                    continue
                project_id = ret_data['data']['advertiser_id']
                creative_id = ret_data['data']['creative_id']
                status_string = ''
                submit_string = ''
                reason = ''
                if ret_data['data']['review_status'] == 'PENDING' :
                    submit_string = 'submited'
                    status_string = 'unaudited' # Unaudited
                elif ret_data['data']['review_status'] == 'APPROVED' :
                    submit_string = 'submited'
                    status_string = 'approved'  # Approved
                    update_kafka_list.append(creative_id)
                    update_item_list.append(str(creative_id))
                elif ret_data['data']['review_status'] == 'REJECTED' :
                    status_string = 'rejected'  # Rejected
                    submit_string = 'unsubmit'
                    if ret_data['data'].has_key('review_msg') and type(ret_data['data']['review_msg']) == unicode :
                        reason = ret_data['data']['review_msg']
                        reason = reason.replace('\'',' ')
		print_list.append({'creative_id':creative_id, 'status':status_string, 'reason':reason})
                sql_cmd = "UPDATE %s SET response_time = '%s', status = '%s'" % (SQL_CREATIVE_TABLE, styled_time_string, status_string)
                if submit_string != '' :
                    sql_cmd += ", submit_status = '%s'" % (submit_string)
                if reason != '' :
                    sql_cmd += ", request_result = '%s'" % (reason.encode('utf-8'))
                if status_string == 'approved' :
                    sql_cmd += ", request_result = ''"
                sql_cmd += " WHERE supplier_id = %d AND creative_set_id = %s" % (CHANNEL_ID, creative_id)
                SQL_CMD_LIST.append(sql_cmd)
        else :
            print 'Query creative : Parse response error.'
            return    
        #print '===SQL_CMD_LIST==='
        #print SQL_CMD_LIST
        if len(update_item_list) <> 0 :
            logging.info('tencent cloud item update:'+str(update_item_list))
            updateItemByCreative(update_item_list)
        if len(update_kafka_list) == 0 :
            QUERY_CREATIVE_LIST = []
            return
        sql_select = 'SELECT DISTINCT(campaign_id) from campaign_creative WHERE creative_set_id IN ('
        while len(update_kafka_list) > 0 :
            creative_id = update_kafka_list.pop()
            sql_select += creative_id
            if len(update_kafka_list) > 0 :
                sql_select += ', '
        sql_select += ')'
        res = SqlSelect(sql_select)
        if res == 'error' :
            QUERY_CREATIVE_LIST = []
            return
        # producer = kafka.KafkaProducer(bootstrap_servers = KAFKA_IP_PORT_LIST)
        for row in res :
            for campaign_id in row :
                #KAFKA_PRODUCER.send(KAFKA_CAMPAIGN_SYNC_TOPIC, str(campaign_id))
		SyncToCampaign(str(campaign_id))
        query_creative_list = []
    if print_result :
	print json.dumps(print_list)
    QUERY_CREATIVE_LIST = []

def ConsumptionReport(start_date, end_date) :
    global QUERY_BID_URL
    global QUERY_EXPOSURE_URL
    global SQL_CONSUMPTION_TABLE
    commit_dict = {}
    commit_dict['data'] = {}
    commit_dict['data']['start_date'] = start_date
    commit_dict['data']['end_date'] = end_date
    now = int(time.time())
    time_array = time.localtime(now)
    styled_time_string = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    request_string = json.dumps(commit_dict, encoding = 'utf-8')
    response_string = Post(QUERY_BID_URL, request_string)
    response_dict = json.loads(response_string, encoding = 'utf-8')
    for report in response_dict['data'] :
	sql_cmd = "INSERT INTO %s(stat_day, channel_id, type, adx_bid, adx_winbid, update_time) VALUES('%s', %d, %d, %d, %d, '%s') ON DUPLICATE KEY UPDATE adx_bid = %d, adx_winbid = %d, update_time = '%s'" % (SQL_CONSUMPTION_TABLE, str(report['date'])[0:4] + '-' + str(report['date'])[4:6] + '-' + str(report['date'])[6:8], CHANNEL_ID, 2, report['bidding'], report['bid_success'], styled_time_string, report['bidding'], report['bid_success'], styled_time_string)
	SQL_CMD_LIST.append(sql_cmd)
    response_string = Post(QUERY_EXPOSURE_URL, request_string)
    print response_string
    response_dict = json.loads(response_string, encoding = 'utf-8')
    for report in response_dict['data'] :
	sql_cmd = "INSERT INTO %s(stat_day, channel_id, type, adx_impression, adx_valid_impression, adx_click, adx_valid_click, adx_cost, update_time) VALUES('%s', %d, %d, %d, %d, %d, %d, %f, '%s') ON DUPLICATE KEY UPDATE adx_impression = %d, adx_valid_impression = %d, adx_click = %d, adx_valid_click = %d, adx_cost = %f, update_time = '%s'" % (SQL_CONSUMPTION_TABLE, str(report['date'])[0:4] + '-' + str(report['date'])[4:6] + '-' + str(report['date'])[6:8], CHANNEL_ID, 2, report['pv'], report['valid_pv'], report['click'], report['valid_click'], report['cost'] / 100.0, styled_time_string, report['pv'], report['valid_pv'], report['click'], report['valid_click'], report['cost'] / 100.0, styled_time_string)
	SQL_CMD_LIST.append(sql_cmd)
	
# main 
ChangeLoggingConfig()

parser = argparse.ArgumentParser()
parser.add_argument('--query_creatives', '--qc', required = False, type = int, nargs = '+', help = 'Query creatives. Need one or more creative id.')
parser.add_argument('--start_date', '--sd', required = False, help = 'Start date of refuse reasons. format yyyy-mm-dd.')
parser.add_argument('--end_date', '--ed', required = False, help = 'End date of refuse reasons. format yyyy-mm-dd.')
parser.add_argument('--advertiser', '--at', required = False, help = 'Advertiser id for refuse reasons.')
parser.add_argument('--creative', '--cr', required = False, help = 'Creative id for refuse reasons.')
parser.add_argument('--query_creatives_info','--qci',required = False, type = int, nargs = '+', help = 'Query creatives info. Need one or more creative id.')
parser.add_argument('--consumption_start_date', '--csd', required = False, help = 'Consumption report. Start date.')
parser.add_argument('--consumption_end_date', '--ced', required = False, help = 'Consumption report. End date.')

args = parser.parse_args()
if type(args.query_creatives) == list :
    is_first = True
    creatives = ''
    for creative_id in args.query_creatives :
	if is_first :
	    is_first = False
	else :
	    creatives += ', '
	creatives += str(creative_id)
    sql_cmd = 'select creative_set_id, project_id from (select creative_set_id, creative_id from creative_set where creative_set_id in (%s)) A join (select id, project_id from creative) B on A.creative_id = B.id' % creatives
    res = SqlSelect(sql_cmd)
    if res == 'error' :
	sys.exit()
    for (creative_id, project_id) in res :
	QUERY_CREATIVE_LIST.append({'creative_id': int(creative_id), 'project_id':int(project_id), str(CHANNEL_ID) + '_ad_type': 'native'})
    queryCreative(print_result = True)
    SqlExecute()
    exit()
elif type(args.start_date) == str and type(args.end_date) == str :
    commit_data = {'data':{'start_date':args.start_date, 'end_date':args.end_date}}
    if type(args.advertiser) == str :
	commit_data['data']['advertiser_id'] = args.advertiser
    if type(args.creative) == str :
	commit_data['data']['creative_id'] = args.creative
    print Post('https://api.e.qq.com/adx/v1/report/bid_fail', json.dumps(commit_data, encoding = 'utf-8'))
    exit()
elif type(args.query_creatives_info) == list :
    queryCreatvieInfo (args.query_creatives_info)
    exit()
elif type(args.consumption_start_date) == str and type(args.consumption_end_date) == str :
    ConsumptionReport(args.consumption_start_date, args.consumption_end_date)
    SqlExecute()
    exit()

consumer = kafka.KafkaConsumer(KAFKA_TOPIC, group_id = KAFKA_GROUP_ID, bootstrap_servers = KAFKA_IP_PORT_LIST, consumer_timeout_ms = KAFKA_TIMEOUT)
ReadOffsetFromFile(consumer)
uncommit_cnt = 0
while 1 :
    ChangeLoggingConfig()
    for message in consumer :
        #print('message:'+str(message))
        try :
            body = json.loads(message.value, encoding = 'utf-8')
        except Exception as e :
            logging.error(e)
            continue
        #print 'body:%s' %body
        #print '=======body:======='
        #print body
        if type(body) != dict :
            logging.error("Wrong type of value. value_type = [%s], value = [%s]" % (type(body), body))
            continue
        if body.has_key('object_type') and body.has_key('operate_type') :
            object_type = body['object_type']
            operate_type = body['operate_type']
        else :
            logging.error("No object_type or operate_type. value = [%s]" % (body))
            continue
        #print type(operate_type)
        if type(object_type) != int or type(operate_type) != unicode :
            print("Wrong type of object_type or operate_type. type(object_type) = [%s], type(operate_type) = [%s]" % (type(object_type), type(operate_type)))
            continue
        if object_type == 1 :
            # Qualification
            if operate_type == 'add' :
                if not body.has_key('advertiser') or type(body['advertiser']) != list :
                    logging.error("New advertiser error.")
                    continue
                for new_adver in body['advertiser'] :
                    if type(new_adver) == dict :
                        if new_adver.has_key('channel') and type(new_adver['channel']) == int and CHANNEL_ID == new_adver['channel']:
                            ADD_ADVERTISER_LIST.append(new_adver)
            elif operate_type == 'modify' :
                if not body.has_key('advertiser') or type(body['advertiser']) != list :
                    logging.error("update advertiser error.")
                    continue
                for update_adver in body['advertiser'] :
                    if type(update_adver) == dict :
                        if update_adver.has_key('channel') and type(update_adver['channel']) == int and CHANNEL_ID == update_adver['channel']:
                            UPDATE_ADVERTISER_LIST.append(update_adver)
            elif operate_type == 'query' :
                if not body.has_key('advertiser') or type(body['advertiser']) != list :
                    logging.error("Query advertiser error.")
                    continue
                for query_adver in body['advertiser'] :
                    if type(query_adver) == dict :
                        if query_adver.has_key('channel') and type(query_adver['channel']) == int and CHANNEL_ID == query_adver['channel'] :
                            QUERY_ADVERTISER_LIST.append(query_adver)
            else :
                logging.error("Unknown operate_type [%s]" % (operate_type))
            # print("operate_type [%s]" % (operate_type))
        elif object_type == 2 :
            # creative
            if operate_type == 'add' :
                if not body.has_key('native') :
                    logging.info('No native body.')
                    continue
                for new_cr in body['native'] :
                    if type(new_cr) == dict :
                        if new_cr.has_key('channel') and type(new_cr['channel']) == int and CHANNEL_ID == new_cr['channel']:
                            new_cr[str(CHANNEL_ID) + '_ad_type'] = 'native'
                            ADD_CREATIVE_LIST.append(new_cr)
            elif operate_type == 'modify' :
                if not body.has_key('native') :
                    print('No native body.')
                    continue
                for new_cr in body['native'] :
                    if type(new_cr) == dict :
                        if new_cr.has_key('channel') and type(new_cr['channel']) == int and CHANNEL_ID == new_cr['channel']:
                            new_cr[str(CHANNEL_ID) + '_ad_type'] = 'native'
                            UPDATE_CREATIVE_LIST.append(new_cr)
            elif operate_type == 'query' :
		if body.has_key('timestamp') and type(body['timestamp']) == int and time.time() - 120 > body['timestamp'] :
		    continue
                if not body.has_key('native') :
                    print('No native body.')
                    continue
                for query_cr in body['native'] :
                    if type(query_cr) == dict :
                        print('query_cr'+str(query_cr))
                        if query_cr.has_key('channel') and type(query_cr['channel']) == int and CHANNEL_ID == query_cr['channel']:  
                            query_cr[str(CHANNEL_ID) + '_ad_type'] = 'native'
                            QUERY_CREATIVE_LIST.append(query_cr)
            else :
                logging.info("Unknown operate_type [%s]" % (operate_type))
        else :
            logging.error("Unknown object_type [%d]" % (object_type))
    # end of test
    addAdvertiser()
    updateAdvertiser()
    queryAdvertiser()
    addCreative()
    updateCreative()
    queryCreative()
    SqlExecute()
    if len(ADD_ADVERTISER_LIST) == 0 and len(UPDATE_ADVERTISER_LIST) == 0 and len(ADD_CREATIVE_LIST) == 0 and len(UPDATE_CREATIVE_LIST) == 0:
        offset_dict = consumer._subscription.all_consumed_offsets()
        for partition_info in offset_dict :
            KAFKA_PARTITION_DICT[str(partition_info.partition)] = offset_dict[partition_info].offset
        WriteOffsetToFile()
        uncommit_cnt = 0
    else :
        uncommit_cnt += 1
        if uncommit_cnt == 3 :
            uncommit_cnt == 0
            for advertiser in ADD_ADVERTISER_LIST :
                logging.error("UNADD ADVERTISER: " + str(advertiser))
            for creative in ADD_CREATIVE_LIST :
                logging.error("UNADD CREATIVE: " + str(creative))
            for advertiser_up in UPDATE_ADVERTISER_LIST :
                logging.error("UNUPDATE ADVERTISER: " + str(advertiser_up))
            for creative_up in UPDATE_CREATIVE_LIST :
                logging.error("UNUPDATE CREATIVE: " + str(creative_up))
    logger.info('~heartbeat~')
    time.sleep(KAFKA_INTERVAL)
    #print("Got %d kafka message" % (message_cnt))
# end of main
