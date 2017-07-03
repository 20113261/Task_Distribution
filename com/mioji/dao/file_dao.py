#!/usr/bin/python
# -*- coding: UTF-8 -*-

'''
Created on 2016年12月30日

@author: dujun
'''

import os
from os import path
import csv
import codecs
import json

cache_dir = path.abspath(path.join(path.dirname(__file__) + '../../../../cache'))
print cache_dir


def cut(rows, count):
    rows_cut = []
    offest = 0
    to = 0
    while to < len(rows):
        to = offest + count
        rows_cut.append(rows[offest:to])
        offest = to
        
    return rows_cut
        

def store_as_csv(file_name, headers, rows, row_count_cut=-1):
    def _store(path, _rows):
        with codecs.open(path, mode='wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(_rows) 
    if row_count_cut > 0 and row_count_cut < len(rows):
        i = 1
        for r in cut(rows, row_count_cut):
            file_path_str = cache_dir + '/' + file_name.format(i)
            _store(file_path_str, r)
            i += 1
    else:
        file_path_str = cache_dir + '/' + file_name
        _store(file_path_str, rows)    


def store_dict(name, result):
    file_path_str = cache_dir + '/' + name
    fo = codecs.open(file_path_str, "wb", 'utf-8')
    fo.write(json.dumps(result, ensure_ascii=False))
    fo.close()


def load_dict(name):
    file_path_str = cache_dir + '/' + name
    if not path.exists(file_path_str):
        return None
    fo = codecs.open(file_path_str, "rb", 'utf-8')
    res = fo.read()
    result = json.loads(res)
    fo.close()
    return result

    
if __name__ == '__main__':
    print load_dict('a')
