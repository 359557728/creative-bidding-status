# -*- coding: UTF-8 -*-
import treq
import logging
import json
import klein

from datetime import datetime

from twisted.internet.task import react
from _utils import print_response

logging.basicConfig(level=logging.INFO, filename='log.dat')

application = klein.Klein()

BIDDING_FAIL_URL = "https://api.e.qq.com/adx/v1/report/bid_fail"
now = datetime.today().strftime('%Y-%m-%d')

@application.route('/')
def demo(request)
    return b'Hello!'

if __name__ == '__main__':
    application.run(debug=True, host='0.0.0.0', port=18075)

