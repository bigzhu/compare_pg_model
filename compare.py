#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ConfigParser
import db


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


def getConnect(db_cfg):
    connect = db.database(port=5432, host=db_cfg['ip'], dbn='postgres', db=db_cfg['db'], user=db_cfg['user'], pw=db_cfg['password'])
    return connect


def getTableList(cfg, connect):
    l = []
    sql = "select tablename from pg_tables where tableowner='%s'" % cfg['user']
    for i in connect.query(sql):
        l.append(i.tablename)
    return l

if __name__ == '__main__':
    cfg1 = readDBInfo('db1')
    cfg2 = readDBInfo('db2')
    connect1 = getConnect(cfg1)
    connect2 = getConnect(cfg2)
    #for i in c1.query(sql):
    #    print i
    tables1 = getTableList(cfg1, connect1)
    tables2 = getTableList(cfg2, connect2)
    just1 = []
    just2 = []
    for i in (set(tables1) ^ set(tables2)):
        if i in tables1:
            just1.append(i)
        else:
            just2.append(i)

    print '只有%s存在以下表:' % cfg1.get('ip')
    for i in just1:
        print i

    print ''
    print ''
    print '只有%s存在以下表:' % cfg2.get('ip')
    for i in just2:
        print i
