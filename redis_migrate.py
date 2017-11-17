#!/usr/bin/env python
import argparse
import redis
import rediscluster
from itertools import izip_longest, izip
from gevent import Timeout
import logging
from datetime import datetime 

BATCH_COUNT = 4096
S_OK, S_ERR = 0, -1

def connect_redis(conn_dict):
    conn = redis.StrictRedis(host=conn_dict['host'],
                             port=conn_dict['port'],
                             db=conn_dict['db'])
    logging.info('Complete to connect to %s:%s', conn_dict['host'], conn_dict['port'])
    return conn

def connect_redis_cluster(conn_dict):
    #host, port, db = conn_dict['host'], conn_dict['port'], conn_dict['db']
    #conn = redis_cluster.RedisClusterEntry(host, [host + ' ' + port + ' ' + str(db)])
    conn = redis.StrictRedisCluster(host=conn_dict['host'],
                             port=conn_dict['port'])
    logging.info('Complete to connect to %s:%s', conn_dict['host'], conn_dict['port'])
    return conn


def conn_string_type(string):
    format = '<host>:<port>/<db>'
    try:
        host, portdb = string.split(':')
        port, db = portdb.split('/')
        db = int(db)
    except ValueError:
        raise argparse.ArgumentTypeError('incorrect format, should be: %s' % format)
    return {'host': host,
            'port': port,
            'db': db}

def __batch_getter(iterable, n = BATCH_COUNT):
    args = [iter(iterable)] * n
    for keys in izip_longest(*args):
        for key in keys:
            if key:
                yield key

def _exec_pipe(the_pipe):
    logging.info('----Begin to process batch----')
    startTime= datetime.now() 
    
    timeout = Timeout(10, False)
    error_code = S_OK
    pipe_result = []
    timeout.start()
    try:
        pipe_result = the_pipe.execute()
    except Timeout, t:
        if t is not timeout:
            raise  # not my timeout
        logging.error('unable to execute the_pipe (possibly gevent.Timeout)')
        error_code = S_ERR
    except Exception as e:
        logging.error('redis pipe fail: cluster. Error: %s', e)
        error_code = S_ERR
    finally:
        timeout.cancel()
        the_pipe.reset()
    
    timeElapsed=datetime.now()-startTime 
    logging.info('Time elpased (hh:mm:ss.ms) {}'.format(timeElapsed))
    logging.info('----complete batch----')
    return error_code, pipe_result


def _migrate_hash(src, dst, key):
    startTime = datetime.now()

    count, pttl, dst_pipe = 0, src.pttl(key), dst.pipeline()
    #dst_pipe.multi()
    for hkey, value in __batch_getter(src.hscan_iter(key, '*')):
        #logging.debug('%s, %s', hkey, value)
        if pttl > 0:
            dst_pipe.psetex(key + ':' + hkey, pttl, value)
        else:
            dst_pipe.set(key + ':' + hkey, value)
        count += 1
        if count == BATCH_COUNT:
            _exec_pipe(dst_pipe)
            count, pttl, dst_pipe = 0, src.pttl(key), dst.pipeline()
            logging.info('Traverse hash key. Time elpased (hh:mm:ss.ms) {}'.format(datetime.now()-startTime))
            startTime = datetime.now()
            #dst_pipe.multi()
    if count > 0:
        _exec_pipe(dst_pipe)
        logging.info('Traverse hash key. Time elpased (hh:mm:ss.ms) {}'.format(datetime.now()-startTime))



def migrate_redis(source, destination):
    src = connect_redis(source)
    dst = connect_redis_cluster(destination)

    key_counter = 0
    batch_key_list = []
    startTime = datetime.now()
    for key in __batch_getter(src.scan_iter('*')):
        if key_counter % BATCH_COUNT == 0:
            print key_counter
            timeElapsed=datetime.now()-startTime 
            logging.info('Traverse key. Time elpased (hh:mm:ss.ms) {}'.format(timeElapsed))
            startTime= datetime.now() 

        if key.startswith('cookie_db'):
            key_counter += 1
            continue
        if key == 'user_db':
            _migrate_user_db(src, dst, key)
        else:
            print key

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=conn_string_type)
    parser.add_argument('destination', type=conn_string_type)
    options = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    src = connect_redis(options.source)
    dst = connect_redis_cluster(options.destination)
    _migrate_hash(src, dst, 'user_db')
    #migrate_redis(options.source, options.destination)

if __name__ == '__main__':
    run()
    print '----Complete----'
