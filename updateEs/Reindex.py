# -*- coding:utf-8 -*-
from datetime import datetime
import getopt
import sys

from elasticsearch import helpers, Elasticsearch, ConnectionTimeout

def main():
    s = "http://10.168.99.124:9200/"
    sindex = "zxpt_content_alias"
    d = "http://10.27.14.162:9200/"
    dindex = "zxpt_content_alias"
    size = 1000

    export(s, sindex, d, dindex, size)

    print "[Done]"


def export(s, sindex, d, dindex, size):
    es = Elasticsearch([s])
    es_des = Elasticsearch([d])

    page = es.search(index=sindex, scroll='2m', size=size)

    try:
        sid = page['_scroll_id']
    except TypeError:
        print "type Error"
        print page
        sys.exit(2)

    scroll_size = page['hits']['total']

    allsize = len(page['hits']['hits'])

    bulkdata(es_des, page['hits']['hits'], dindex, size)

    while scroll_size > 0:
        printSth("Scrolling...")
        page = getpage(es, sid)
        # Update the scroll ID
        sid = page['_scroll_id']

        scroll_size = len(page['hits']['hits'])

        allsize += scroll_size
        printSth("scroll size: " + str(scroll_size) + "  all size: " + str(allsize) + "  \r\n")

        bulkdata(es_des, page['hits']['hits'], dindex, size)

    print("\n" + sid)

    es.clear_scroll(sid)

def bulkdata(es_des, pagedata, dindex, size):
    f = pagedata
    for a in f:
        a["_index"] = dindex
    try:
        helpers.bulk(es_des, f)
    except  ConnectionTimeout as error:
        printSth("time out retry\r\n")
        bulkdata(es_des, pagedata, dindex, size)



def getpage(es, sid):
    try:
        page = es.scroll(scroll_id=sid, scroll='2m')

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
