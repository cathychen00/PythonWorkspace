# -*- coding:utf-8 -*-
from datetime import datetime
import getopt
import sys

from elasticsearch import helpers, Elasticsearch, ConnectionTimeout


def main():
    s = ["http://10.168.99.124:9200", "http://10.168.99.122:9200", "http://10.168.99.83:9200"]
    sindex = "zxpt_content_alias"
    size = 5000
    fieldName = "vvCount"
    defaultFieldName = "viewCount"

    export(s, sindex, size, fieldName, defaultFieldName)

    print "[Done]"


def export(s, sindex, size, fieldName, defaultFieldName):
    es = Elasticsearch(s)
    page = es.search(index=sindex, scroll='5m', size=size, body={
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "bizType": {
                                "value": "41"
                            }
                        }
                    },
                    {
                        "range": {
                            "bizId": {
                                "gte": 420000,
                                "lte": 430000
                            }
                        }
                    }
                ],
                "must_not": {
                    "exists": {
                        "field": "vvCount"
                    }
                }
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

    bulkdata(es, page['hits']['hits'], size, fieldName, defaultFieldName)

    while scroll_size > 0:
        printSth("Scrolling...")
        page = getpage(es, sid)
        # Update the scroll ID
        sid = page['_scroll_id']

        scroll_size = len(page['hits']['hits'])

        allsize += scroll_size
        printSth("scroll size: " + str(scroll_size) + "  all size: " + str(allsize) + "  \r\n")

        bulkdata(es, page['hits']['hits'], size, fieldName, defaultFieldName)

    print("\n" + sid)

    es.clear_scroll(sid)


def bulkdata(es_des, pagedata, size, fieldName, defaultFieldName):
    f = pagedata
    for a in f:
        if((defaultFieldName) in a["_source"].keys()):
            a["_source"][fieldName] = a["_source"][defaultFieldName + "a"]
    try:
        helpers.bulk(es_des, f)
    except  ConnectionTimeout as error:
        printSth("time out retry\r\n")
        bulkdata(es_des, pagedata, size,fieldName,defaultFieldName)


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
