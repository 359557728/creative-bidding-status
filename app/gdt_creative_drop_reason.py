# -*- coding: UTF-8 -*-
import treq
import logging
import json
import klein

from datetime import datetime
from twisted.web import http
from twisted.internet import reactor, task
from twisted.web.client import HTTPConnectionPool

logging.basicConfig(level=logging.INFO, filename='log.dat')

application = klein.Klein()

BIDDING_FAIL_URL = "https://api.e.qq.com/adx/v1/report/bid_fail"
now = datetime.today().strftime('%Y-%m-%d')

pool = HTTPConnectionPool(reactor)

@application.route('/')
def gdtBiddingFailReason(request):
    deferred = treq.post(BIDDING_FAIL_URL, data = {"start_date": now, "end_date", now}, headers={"Content-Type": "application/json", "Authorization": "Bearer NTYyMzM4LDE0MzE5MzY5ODIsMGQzMWUyYTRjNmY1ZjEzZDdmY2ZmODVjYjU4YTQ0ZTY="}, pool=pool)
    deferred.addCallback(request_done)
    return deferred

def request_done(response):
    deferred = treq.json_content(response)
    deferred.addCallback(body_received)
    deferred.addErrback(lambda x: None) # ignore errors
    return deferred

def body_received(body):
    logging.info(body)

if __name__ == '__main__':
    application.run('0.0.0.0', 18075)
