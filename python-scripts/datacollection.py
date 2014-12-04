
# coding: utf-8

## Data Processing scripts

# fetch followers and friends for twitter accounts

# In[2]:

#get db connection
from pymongo import MongoClient
import time

#dbconn = MongoClient('localhost', 27017)
dbconn = MongoClient('mongodb://nirmal:rsnd9865@ds059710-a0.mongolab.com:59710/twitter_ethnicity')
db = dbconn.twitter_ethnicity

print db


# In[12]:

#helper functions
def get_credentials():
    """Gets twitter credentials from DB 
    Returns:
        Respone dict , or None if failed.
    """
    twitter_cred = db['twitter_cred']
    return twitter_cred.find_and_modify(query={'is_taken':False}, update={'$set':{'is_taken':True}}, upsert=False, sort=None)

def get_prime_accounts_to_process():
    """Gets unprocessed prime accounts from DB
    Returns:
        prime dict or None
    """
    prime_account = db['prime_accounts']
    prime_accounts = prime_account.find_and_modify(query={'is_processed':False,'is_taken':False}, update={'$set':{'is_taken':True}}, upsert=False, sort=None, full_response=False)
    return prime_accounts

def get_follower_to_process():
    """Gets unprocessed followers form DB
    Returns:
        follower details dict or None
    """
    xmatrix = db['xmatrix']
    followersDtls = xmatrix.find_and_modify(query={'is_processed':False,'is_taken':False}, update={'$set':{'is_taken':True}}, upsert=False, sort=None, full_response=False)
    if followersDtls:
        followersDtls['follower_id'] = followersDtls.pop('_id')
    return followersDtls

def add_followers_to_db(user_id,followers_ids):
    """Adds followers to DB
    Args:
        user_id     :user id of which the user follows
        followers_ids: Ids of followers of the prime accounts
    """
    xmatrix = db['xmatrix']
    for follower_id in followers_ids:
        xmatrix.update({'_id':follower_id},{'$addToSet':{'follows':user_id},'$set':{'is_processed':False,'is_taken':False}},True)

def add_friends_to_dB(follower_id,friends_list):
    """Adds friends to DB
    Args:
        follower_id: Id of follower 
        freiends   : list of his friends
    """
    xmatrix = db['xmatrix']
    xmatrix.update({'_id':follower_id},
                            {'$addToSet':{
                                'follows':{
                                    '$each':friends_list
                                }
                            }
                                          
                        })

def update_processed_flag(user_id):
    """updates processed flag to true
    Args:
        user_id: user id for which processed flag to be updated
    """
    xmatrix = db['xmatrix']
    xmatrix.update({'_id':user_id},{'$set':{'is_taken':False,'is_processed':True}})
    
def remove_user_from_x(self,user_id):
    """removes user from xmatrix
    Args:
        user_id: user id 
    """
    xmatrix = db['xmatrix']
    xmatrix.remove({'_id':user_id})

def robust_request(twitter, resource, params, max_tries=5):
    """ If a Twitter request fails, sleep for 15 minutes.
    Do this at most max_tries times before quitting.
    Args:
      twitter .... A TwitterAPI object.
      resource ... A resource string to request.
      params ..... A parameter dictionary for the request.
      max_tries .. The maximum number of tries to attempt.
    Returns:
      A TwitterResponse object, or None if failed.
    """
    for i in range(max_tries):
        request = twitter.request(resource, params)
        if request.status_code == 200:
            return request
        r = [r for r in request][0]
        if ('code' in r and r['code'] == 34) or ('error' in r and r['error'] == 'Not authorized.'):   # 34 == user does not exist.
            print >> sys.stderr, 'skipping bad request', resource, params
            return None
        else:
            print >> sys.stderr, 'Got error:', request.text, '\nsleeping for 15 minutes.'
            sys.stderr.flush()
            time.sleep(60 * 15)


    


# In[6]:

#get twitter obj 
import sys
from TwitterAPI import TwitterAPI
twitter = get_credentials()
if twitter:
    twitterObj = TwitterAPI(
            twitter['api_key'],
            twitter['api_secret'],
            twitter ['access_key'],
            twitter['access_secret'])
else:
    print >> sys.stderr,'Twitter credits not available'


# In[19]:

def get_followers(user_id,count=300,cursor=-1):
    """To get followers of given twitter Ids
    Args:
        user_id... twitter user id
    Returns
        followers list
    """
    followers = []
    request = robust_request(twitterObj, 'followers/ids',
                             {'user_id': user_id, 'count': count, 'cursor' : cursor ,'stringify_ids' :True })
    if request:
        for result in request:
            if 'ids' in result:
                followers += result['ids']
    return followers


def get_friends(user_id,count=5000):
    """To get friends of given twitter Ids
    Args:
        user_id... twitter user id
    Returns
        friends list
    """
    friends = []
    request = robust_request(twitterObj, 'friends/ids',
                             {'user_id': user_id, 'count': count, 'stringify_ids' :True})
    if request:
        for result in request:
            if 'ids' in result:
                friends += result['ids']
    return friends


# In[20]:

#For testing purpose
#else set it to -1
cut_off = 3


# In[23]:

import sys

def process_followers():
    """Gets unprocessed prime accounts from DB 
    fetch 300 followers of those prime accounts and 
    adds it to DB. 
    Halts when all accounts are processed or cutt_off is reached
    """
    global cut_off
    while True:
        if cut_off == 0:
            print >> sys.stderr, 'cut off reached'
            break
        cut_off -= 1
        prime_account = get_prime_accounts_to_process()
        if not prime_account:
            print >> sys.stderr, 'No prime accounts to process'
            break
        followers = get_followers(prime_account['user_id'])
        if len(followers) > 0:
            add_followers_to_db(str(prime_account['user_id']),followers)
            print 'added %d followers of %s to DB'%(len(followers),prime_account['user_id'])
#process_followers()


# In[24]:

#For testing purpose
#else set it to -1
cut_off = -1


# In[25]:

import sys
def process_friends():
    """Gets unprocessed follower and get 
    5000 of their friends , adds it to DB
    Halts when all followers are processed or cutt_off is reached
    """
    global cut_off
    while True:
        if cut_off == 0:
            print >> sys.stderr, 'cut off reached'
            break
        cut_off -= 1
        follower = get_follower_to_process()
        if not follower:
            print >> sys.stderr, 'No follower to process'
            break
        friends = get_friends(str(follower['follower_id']))
        if len(friends) > 0:
            add_friends_to_dB(follower['follower_id'],friends)
            print 'added %d followers of %s to DB'%(len(friends),follower['follower_id'])
            update_processed_flag(str(follower['follower_id']))
        else:
            remove_user_from_x(str(follower['follower_id']))
            print >> sys.stderr, 'removed user , unable to fetch friends list for %s'%(str(follower['follower_id']))

process_friends()


# In[ ]:



