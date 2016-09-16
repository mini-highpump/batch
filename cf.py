#!/usr/bin/python
#coding: utf-8
import math
import redis
from comm import USER_VECTOR_KEY_PREFIX, USER_RESULT_KEY_PREFIX, USER_HASH_KEY_PREFIX, \
                 SONG_HASH_KEY_PREFIX, BATCH_CF_CONFIG
'''
Collaborative Filtering.
Load user perference matrices from redis and compute every user's simliarity.
'''


def euclidean_sim(vA, vB):
    '''
    Compute euclidean simliarity between vector A to vector B
    @return simliarity of these two vectors
    '''
    sum = 0
    for (x, y) in zip(vA, vB):
        sum = (x - y) ** 2
    return 1.0 / (1 + math.sqrt(sum))


def pickup_by_threshold(v, threshold):
    '''
    Pick up from list v by threshold. (which is greater than threshold)
    '''
    result = []
    for item in v:
        if item > threshold:
            result.append(v.index(item))
    return result


def run_diff(s, *args):
    if args == ():
        raise StopIteration()
    first = args[0]
    yield [i for i in first if i not in s]
    run_diff(s, *args[1:]) # jump over the first one
    

class CFDriver(object):
    '''
    Collaborative filtering driver.
    It is a data-driving model.
    '''
    def __init__(self):
        self.r = redis.Redis("localhost")
        self.sidhash = self.r.hgetall(SONG_HASH_KEY_PREFIX)
        self.uidhash = self.r.hgetall(USER_HASH_KEY_PREFIX)
        self.config = self.r.hgetall(BATCH_CF_CONFIG)
        self.total_user = self.config["total_user"]
        self.total_song = self.config["total_song"]
        # N * N metric
        self.simetric = [[0 for i in xrange(self.total_user)] for i in xrange(self.total_user)]


    def compute(self):
        '''
        Compute simliarity.
        '''
        total_user = self.config["total_user"]
        for i in xrange(total_user):
            for j in xrange(i, total_user):
                uidA = self.uidhash[i]
                uidB = self.uidhash[j]
                vA = self.r.lrange(USER_VECTOR_KEY_PREFIX + uidA, 0, -1)
                vB = self.r.lrange(USER_VECTOR_KEY_PREFIX + uidB, 0, -1)
                sim = euclidean_sim(vA, vB)
                # 这里实际上是对角矩阵
                self.simetric[i][j] = sim
                self.simetric[j][i] = sim


    def pickup(self, i):
        '''
        Pickup neighborhoods for user i.
        '''
        return pickup_by_threshold(self.simetric[i], 0.5) # user index vector


    def recommend(self, i):
        '''
        Recommend items for user i.
        '''
        uid = self.uidhash[i]
        s = pickup_by_threshold(self.r.lrange(USER_VECTOR_KEY_PREFIX + uid, 0, -1), 0.5) # song index vector
        r = []
        for neighbor in pickup():
            uid_t = self.uidhash[neighbor]
            r.append(pickup_by_threshold(self.r.lrange(USER_VECTOR_KEY_PREFIX + uid_t, 0, -1), 0.5))
        result = []
        for n in run_diff(s, *r):
            result.push_back(i for i in n)
        return set(result)
        

    def write_result(self, i, s):
        '''
        Write all result back to redis.
        '''
        uid = self.uidhash[i]
        self.r.rpush(USER_RESULT_KEY_PREFIX + uid, *[self.sidhash[j] for j in list(s)])


    def run(self):
        '''
        Connect these operation and do all things.
        '''
        self.compute()
        for i in xrange(total_user):
            s = self.recommend(i)
            self.write_result(i, s)


if __name__ == "__main__":
    cf = CFDriver()
    cf.run()
