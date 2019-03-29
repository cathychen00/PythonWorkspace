# -*- coding:utf-8 -*-
from datetime import datetime
import getopt
import sys

from elasticsearch import helpers, Elasticsearch, ConnectionTimeout


def main():
    s = ["http://10.27.14.162:9200/"]
    sindex = "zxpt_content_alias"
    size = 5000

    export(s, sindex, size)

    print "[Done]"


def export(s, sindex, size):
    es = Elasticsearch(s)
    page = es.search(index=sindex, scroll='5m', size=size, body={
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "bizType": {
                                "gte": 30,
                                "lte": 40
                            }
                        }
                    }
                ],
                "must_not": [
                    {
                        "exists": {
                            "field": "seriesColumnId"
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "bizId": {
                    "order": "asc"
                }
            }
        ]
    })

    try:
        sid = page['_scroll_id']
    except TypeError:
        print "type Error"
        print page
        sys.exit(2)

    scroll_size = page['hits']['total']

    allsize = len(page['hits']['hits'])

    bulkdata(es, page['hits']['hits'], size)

    while scroll_size > 0:
        printSth("Scrolling...")
        page = getpage(es, sid)
        # Update the scroll ID
        sid = page['_scroll_id']

        scroll_size = len(page['hits']['hits'])

        allsize += scroll_size
        printSth("scroll size: " + str(scroll_size) + "  all size: " + str(allsize) + "  \r\n")

        bulkdata(es, page['hits']['hits'], size)

    print("\n" + sid)

    es.clear_scroll(sid)


def convertSeriesColumn(tagNames):
    if tagNames is None or len(tagNames) == 0:
        return 0
    if u"最新车讯" in tagNames or u"品牌分析" in tagNames:
        return 10001
    elif u"评测体验" in tagNames or  u"海选导购" in tagNames:
        return 10002
    else :
        return 0


def bulkdata(es_des, pagedata, size):
    f = pagedata
    for a in f:
        if "tagNames" not in a["_source"]:
            continue
        tagNames = a["_source"]["tagNames"]
        seriesColumnId = convertSeriesColumn(tagNames)
        a["_source"]["seriesColumnId"] = seriesColumnId
    try:
        helpers.bulk(es_des, f)
    except  ConnectionTimeout as error:
        printSth("time out retry\r\n")
        bulkdata(es_des, pagedata, size)


def getpage(es, sid):
    try:
        page = es.scroll(scroll_id=sid, scroll='5m')

        if page is None:
            print "page None try again:"
            getpage(es, sid)

        try:
            sid = page['_scroll_id']
        except TypeError:
            print "type Error"
            print page
            sys.exit(2)

        return page
    except  ConnectionTimeout as error:
        printSth("getpage time out retry\r\n")
        getpage(es, sid)


def printSth(sth):
    sys.stdout.write("\r%s %s" % (datetime.now(), sth))
    sys.stdout.flush()


if __name__ == '__main__':
    main()
