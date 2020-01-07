# -*- coding: UTF-8 -*-
import treq
import logging
import json
import klein
import mysql.connector
from twisted.web import http
from twisted.internet import reactor, task
from twisted.web.client import HTTPConnectionPool

logging.basicConfig(level=logging.INFO, filename='log.dat')

application = klein.Klein()

BIDDING_FAIL_URL = "https://api.e.qq.com/adx/v1/report/bid_fail"

pool = HTTPConnectionPool(reactor)

mysqldb = mysql.connector.connect(host = "123.59.18.197", user = "dsp", password = "DSPprdDSP", database = "dspprd", port = "3406")

@application.route('/gdt/<string:date>')
def gdtBiddingFailReason(request, date):
    deferred = treq.post(BIDDING_FAIL_URL, json = {"data" : {"start_date": date, "end_date": date}}, headers={"Content-Type": "application/json", "Authorization": "Bearer NTYyMzM4LDE0MzE5MzY5ODIsMGQzMWUyYTRjNmY1ZjEzZDdmY2ZmODVjYjU4YTQ0ZTY="}, pool=pool)
    deferred.addCallback(lambda response, date: request_done(response, date))
    return deferred

def request_done(response, date):
        logging.info('date accept {}'.format(date))
    deferred = treq.text_content(response, encoding="utf-8")
    deferred.addCallback(lambda data, date: body_received(data, date))
    deferred.addErrback(lambda x: None) # ignore errors
    return deferred

def body_received(data, date):
        logging.info('data {} date {}'.format(data, date))
    try:
        cursor = mysqldb.cursor()
        maybeContinuable = json.loads(data)
        if maybeContinuable['code'] == 0:
                records = maybeContinuable['data']
                for item in records:
                        cursor.execute("INSERT INTO yxt_gdt_bid_fail_report (bid_count, reason_message, reason_code, date) VALUES (%d, %s, %d, %s) ON DUPLICATE KEY UPDATE VALUES bid_count =%d, reason_message = %s, reason_code = %d", (item['bid_count'], item['reason_message'], item['reason_code'], date))
    except Exception as e:
        logging.error(e)


if __name__ == '__main__':
    application.run('0.0.0.0', 18075)
