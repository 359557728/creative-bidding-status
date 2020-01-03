# -*- coding: UTF-8 -*-
import treq
import logging
import json
import klein

from datetime import datetime
from twisted.web import http

logging.basicConfig(level=logging.INFO, filename='log.dat')

application = klein.Klein()

BIDDING_FAIL_URL = "https://api.e.qq.com/adx/v1/report/bid_fail"
now = datetime.today().strftime('%Y-%m-%d')

@application.route('/')
def gdtBiddingFailReason(request):
    response = yield treq.post(BIDDING_FAIL_URL, json.dumps({"start_date": now, "end_date", now}).encode('ascii'), headers={b'Content-Type': [b'application/json'], "Authorization": "Bearer NTYyMzM4LDE0MzE5MzY5ODIsMGQzMWUyYTRjNmY1ZjEzZDdmY2ZmODVjYjU4YTQ0ZTY="})
    if response.code != http.OK:
        reason = http.RESPONSE[response.code]
    content = yield response.content()
    print(content)
    return content

if __name__ == '__main__':
    application.run('0.0.0.0', 18075)
