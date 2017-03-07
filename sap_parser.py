# -*- coding: utf8 -*-
""" 
    Парсинг лога fiddler и загрузка в БД

    Таблица:
    drop table mk$oapi_requests;
    create table mk$oapi_requests(req_id number(8),request varchar2(128), params varchar2(128));
    select * from mk$oapi_requests

"""

import sys, tempfile, shutil, zipfile, os
import cx_Oracle
from bs4 import BeautifulSoup
import json

def create_temp_dir():
    try:
        return tempfile.mkdtemp()
    except:
        print "failed to create temp directory for saz extraction"
        sys.exit(-1)

def clean_temp_dir(tmpdir):
    if tmpdir: 
        try:
            shutil.rmtree(tmpdir)
        except:
            print "failed to clean up tmpdir %s you will have to do it" % (tmpdir)

def unzip_archive(archive, tmpdir):
    try:
        z = zipfile.ZipFile(archive,"r")
    except:
        print "failed to open saz file %s" % (archive)
        sys.exit(-1)
    try:
       z.extractall(tmpdir)
       z.close()
    except:
       print "failed to extract saz file %s to %s" % (archive, tmpdir)
       sys.exit(-1)

def parse_request_file(tmpdir, req_file):
    with open(os.path.join(tmpdir, req_file), 'r') as data:
        for line in data:
            if 'functions' in line:
                return line

def parse_index_file(tmpdir):
    with open(os.path.join(tmpdir, '_index.htm'), 'r') as data:
        requests = []
        soup = BeautifulSoup(data)
        for row in soup.findAll('tr')[1:]:
            req_id = int(row.find('td').find('a')['href'].split('\\')[1].split('_')[0])
            td = row.findAll('td')[5].text
            if 'authToken=' in td:
                link = td.split('authToken=')[0] + 'authToken=%s&' + td.split('authToken=')[1].split('&')[1]
                print link
                requests.append({'req_id': req_id, 'request': link, 'params': ''})
            elif 'batchExecuteAsync' in td:
                print td
                requests.append({'req_id': req_id, 'request': td, 'params': ''})
                req_file = row.findAll('td')[0].findAll('a')[0]['href']
                print 'Used request file: %s' % req_file
                batch_requests = parse_request_file(tmpdir, req_file)
                batch_requests = json.loads(batch_requests)
                for function in batch_requests['functions']:
                    requests.append({'req_id': req_id, 'request': function['url'], 'params': function['params']})
            else:
                print td
                requests.append({'req_id': req_id, 'request': td, 'params': ''})
        return requests

def upload_to_db(requests):
    print 'Upload to DB'
    connection = cx_Oracle.connect("bis/bisbisbis00@bistst2")
    cursor = connection.cursor()
    cursor.execute("""truncate table mk$oapi_requests""")
    for i in xrange(len(requests)):
        print i, requests[i]
        cursor.execute("""INSERT INTO mk$oapi_requests(req_id, request, params) 
                VALUES (:req_id, :request, :params)""",
               {
                'req_id' : requests[i]['req_id'],
                'request' : requests[i]['request'],
                'params' : str(requests[i]['params']),
               }
              )
    connection.commit()
    connection.close()
    print 'Done!'


def work_with_archive(archive, tmpdir):
    unzip_archive(archive, tmpdir)
    requests = parse_index_file(tmpdir)
    upload_to_db(requests)

if __name__ == '__main__':
    if (len(sys.argv) > 1):
        print 'Start saz parser'
        print 'Parsing file %s ' % sys.argv[1]
        tmpdir = create_temp_dir()
        print 'Temp dir: %s' % tmpdir
        work_with_archive(sys.argv[1], tmpdir)
        clean_temp_dir(tmpdir)
        print 'Stop saz parser'
    else:
        print u'Usage ./saz_parser.py {file}'