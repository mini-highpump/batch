#!/usr/bin/python
#coding: utf-8
from comm import USER_VECTOR_KEY_PREFIX, USER_RESULT_KEY_PREFIX, SONG_HASH_KEY_PREFIX, \
                 USER_HASH_KEY_PREFIX, BATCH_CF_CONFIG
import redis
import MySQLdb
import time

'''
Load data from db and write to redis after pretreatment.
'''


class Pretreatment(object):
    '''
    Pretreatment.
    '''
    def __init__(self):
        self.r = redis.Redis("localhost")
        self.db = MySQLdb.connect("localhost", "root", "a767813944", "highpump")
        self.uidhash = {}
        self.sidhash = {}
        self.total_user = 0
        self.total_song = 0


    def __del__(self):
        self.r.hmset(BATCH_CF_CONFIG, {"total_user":self.total_user, "total_song":self.total_song})
        self.db.close()


    def load_user(self):
        '''
        Load all users.
        '''
        cursor = self.db.cursor()
        cursor.execute("select uid from highpump.t_user_info")
        results = cursor.fetchall()
        index = 0
        for row in results:
            self.uidhash[index] = row[0]
            index += 1
        self.total_user = index
        self.r.hmest(USER_HASH_KEY_PREFIX, self.uidhash)


    def load_song(self):
        '''
        Load all songs.
        '''
        cursor = self.db.cursor()
        cursor.execute("select sid from highpump.t_song_info")
        results = cursor.fetchall()
        index = 0
        for row in results:
            self.sidhash[index] = row[0]
            index += 1
        self.total_song = index
        self.r.hmset(SONG_HASH_KEY_PREFIX, self.sidhash)


    def load_favor(self, uid):
        '''
        Load favorate table.
        '''
        cursor = self.db.cursor()
        cursor.execute("select sid from highpump.t_favor_list where uid=%s and state=1" % uid)
        results = [row[0] for row in cursor.fetchall()]
        r = []
        for i in xrange(self.total_song):
            if self.sidhash[i] in results:
                r.append(1)
            else:
                r.append(0)
        return r


    def load_history(self, uid):
        '''
        Load play list table.
        '''
        cursor = self.db.cursor
        cursor.excute("select sid, start_time, cost_time from highpump.t_play_list where uid=%s" % uid)
        results = cursor.fetchall()
        d = {}
        for row in results:
            d[row[0]] = []
            d[row[0]].append({"start_time":row[1], "cost_time":row[2]})
        
        r = []
        for i in xrange(self.total_song):
            if d.has_key(self.sidhash[i]):
                score = 0
                for item in d[self.sidhash[i]]:
                    t = 0
                    if item["cost_time"] < 20:
                        t = -1 
                    else:
                        t = 0.7;
                    starttime = int(time.mktime(time.strptime(item["start_time"], "%Y-%m-%d %H:%M:%S")))
                    curtime = int(time.time())
                    diff = curtime - starttime
                    if diff > 2419200: # greater than 4 weeks
                        t *= 0.15
                    elif diff > 604800 and diff < 2419200: # between 1 week and 4 weeks
                        t *= 0.35
                    else: # less than 1 week
                        t *= 0.5
                    score += t
                r.append(score)
            else:
                r.append(0)
        return r


    def normalized(self, v):
        '''
        Normalized vector v.
        '''
        sum = reduce(lambda x,y:x+y, v)
        map(lambda x: x/float(sum), v)


    def write_result(self, i, v):
        '''
        Write data to redis.
        '''
        self.r.rpush(USER_VECTOR_KEY_PREFIX + self.uidhash[i], *v)


    def run(self):
        '''
        Run pretreatment.
        '''
        self.load_user()
        self.load_song()
        for i in xrange(self.total_user):
            uid = self.uidhash[i]
            f = self.load_favor(uid)
            h = self.load_history(uid)
            r = [i+j for i in f for j in h]
            self.normalized(r)
            self.write_result(i, r)


if __name__ == "__main__":
    app = Pretreatment()
    app.run()
