
# coding: utf-8

## Data loading scripts

# contains scripts to load required data to database required for processing

# In[44]:

#get db connection
from pymongo import MongoClient

dbconn = MongoClient('localhost', 27017)
dbconn.drop_database('twitter_ethnicity') #drop database if exists
db = dbconn.twitter_ethnicity

print db


# In[45]:

"""Reads twitter credentials from twitterCreds.txt and 
    adds it to DB
"""
def load_twitter_credentials():
    twitter_cred = db['twitter_cred']
    with open('../data/twitterCreds.txt') as f:
        for line in f.readlines():
            creds = line.split()
            if len(creds) != 4:
                print 'Bad line ignoring...'
            else:
                cred_names = ['access_key','access_secret','api_key','api_secret','is_taken']
                creds += [False]
                cred_obj = dict(zip(cred_names, creds))
                twitter_cred.insert(cred_obj)
                print 'inserted into DB...'


load_twitter_credentials()    


# In[48]:

"""Loads details of twitter accounts for 
which we want to fetch followers
"""
def load_prime_accounts():
    prime_accounts = db['prime_accounts']
    with open('../data/primeAccounts.txt') as f:
        prime_accounts = db['prime_accounts']
        for line in f.readlines():
            values = line.split(',')
            if len(values) != 4:
                print 'Bad line ingnoring...'
            else:
                keys = ['user_id','screen_name','desc','count','is_processed','is_taken','cursor']
                values = values + [ False, False, -1]
                prime_obj = dict(zip(keys, values))
                prime_accounts.insert(prime_obj)
                print 'inserted into DB...'
    

load_prime_accounts()


# In[ ]:



