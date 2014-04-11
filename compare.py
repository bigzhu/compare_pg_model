#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ConfigParser
import db
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from utils import Storage
table_sql = ''


def readDBInfo(db_cfg):
    config = ConfigParser.ConfigParser()
    dic = {}
    with open('db.ini', 'r') as cfg_file:
        config.readfp(cfg_file)
        dic['user'] = config.get(db_cfg, 'user')
        dic['password'] = config.get(db_cfg, 'password')
        dic['ip'] = config.get(db_cfg, 'ip')
        dic['db'] = config.get(db_cfg, 'db')
    return dic


def readSQL():
    global table_sql
    config = ConfigParser.ConfigParser()
    with open('db.ini', 'r') as cfg_file:
        config.readfp(cfg_file)
        table_sql = config.get('table_sql', 'and')


def getConnect(db_cfg):
    connect = db.database(port=5432, host=db_cfg['ip'], dbn='postgres', db=db_cfg['db'], user=db_cfg['user'], pw=db_cfg['password'])
    return connect


def getTableList(cfg, connect):
    l = []
    sql = "select tablename from pg_tables where tableowner='%s' " % cfg['user']
    sql += table_sql
    for i in connect.query(sql):
        l.append(i.tablename)
    return l


def getTableColum(table_name, connect):
    sql = '''
        select format_type(a.atttypid,a.atttypmod) as type,a.attname as name
        from pg_class as c,pg_attribute as a
        where c.relname = '%s' and a.attrelid = c.oid and a.attnum>0 and a.atttypid<>0
    ''' % table_name
    return list(connect.query(sql))

if __name__ == '__main__':
    cfg1 = readDBInfo('db1')
    cfg2 = readDBInfo('db2')
    readSQL()
    connect1 = getConnect(cfg1)
    connect2 = getConnect(cfg2)
    #for i in c1.query(sql):
    #    print i

    #比较表差异
    tables1 = getTableList(cfg1, connect1)
    tables2 = getTableList(cfg2, connect2)
    just1 = [x for x in tables1 if x not in tables2]
    just2 = [x for x in tables2 if x not in tables1]

    diff_tables = just1 + just2
    all_have_tables = [x for x in tables1 if x not in diff_tables]
    for i in diff_tables:
        if i in tables1:
            just1.append(i)
        else:
            just2.append(i)

    print ''
    print 'h2. 只有%s存在以下表' % cfg1.get('ip')
    print ''
    for i in just1:
        print '|' + i + '|'

    print ''
    print 'h2. 只有%s存在以下表' % cfg2.get('ip')
    print ''
    for i in just2:
        print '|' + i + '|'

    #比较表字段
    print ''
    print 'h2. 表字段差异'
    print ''
    print '|_. table_name|_. 数据库|_. 字段名|_. 字段类型|'
    for table_name in all_have_tables:
        table_colum1 = getTableColum(table_name, connect1)
        table_colum2 = getTableColum(table_name, connect2)
        just_colum1 = [x for x in table_colum1 if x not in table_colum2]
        just_colum2 = [x for x in table_colum2 if x not in table_colum1]
        if len(just_colum1) != 0:
            for i in just_colum1:
                print '|%s|%s|%s|%s|' % (table_name, cfg1.get('ip'), i.name, i.type)
        if len(just_colum2) != 0:
            for i in just_colum2:
                print '|%s|%s|%s|%s|' % (table_name, cfg2.get('ip'), i.name, i.type)
