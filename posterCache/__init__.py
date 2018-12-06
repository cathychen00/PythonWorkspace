import urllib
import urllib2
import time

def visit(id):
    print(id)
    url="http://www.test.com/GetAttentionPoster?_appid=test&authorId=%d&nocache=False"%(id)
    print(url)
    req = urllib2.Request(url)
    res_data = urllib2.urlopen(req)
    res = res_data.read()
    print res
    print('\r\n')
    time.sleep(1)

def process():
    file = open("ids.txt")
    for line in file:
        visit(int(line))

process()