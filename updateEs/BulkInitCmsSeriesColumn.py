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
                        "terms": {
                            "bizType": [
                                11, 12
                            ]
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


def convertSeriesColumn(kind, classId1, classId2):
    isNews = classId1 == 1 and (classId2 == 66 or classId2 == 74)
    isReviews = kind == 1 and (classId1 == 3 or classId1 == 60)
    isOthers = kind == 1 and (classId1 == 82 or classId1 == 97 or classId1 == 102 or classId1 == 107)
    if (isNews):
        return 10001
    elif isReviews:
        return 10002
    elif isOthers:
        return 0
    else:
        return -1


def bulkdata(es_des, pagedata, size):
    f = pagedata
    for a in f:
        seriesColumnId = 0
        if "cms_classId1" in a["_source"]:
            kind=a["_source"]["cms_kind"]
            classId1 = a["_source"]["cms_classId1"]
            classId2 = a["_source"]["cms_classId2"]
            seriesColumnId = convertSeriesColumn(kind,classId1, classId2)
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
