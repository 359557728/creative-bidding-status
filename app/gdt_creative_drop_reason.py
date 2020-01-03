# -*- coding: UTF-8 -*-
import treq
import logging
import json
import klein

from datetime import datetime

logging.basicConfig(level=logging.INFO, filename='log.dat')

application = klein.Klein()

BIDDING_FAIL_URL = "https://api.e.qq.com/adx/v1/report/bid_fail"
now = datetime.today().strftime('%Y-%m-%d')

@application.route('/')
def demo(request):
    return b'Hello!'

if __name__ == '__main__':
    application.run('0.0.0.0', 18075)

