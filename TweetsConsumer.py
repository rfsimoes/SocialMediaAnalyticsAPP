from pymongo import MongoClient
import datetime
import boto.sqs
from boto.sqs.message import Message
import cPickle
from datetime import date
import json
from topia.termextract import extract
from collections import OrderedDict
import re
import ast
import random
import sys
import unicodedata

parseaddic = {}
aggregatedic = {}
TERMS = {}
global conn
global q
global client
global db
global keyword

######## Setup Consumer Function ########
def setupConsumer():
    global conn
    global q
    global client
    global db
    global keyword

    # _____ Initialize keyword list _____
    keyword = 'india'

    #_____ Load Sentiments Dict _____
    sent_file = open('AFINN-111-2.txt')
    sent_lines = sent_file.readlines()
    for line in sent_lines:
        s = line.split(".")
        TERMS[s[0]] = s[1]
    sent_file.close()

    #_____ Connect to SQS Queue _____
    conn = boto.sqs.connect_to_region(
        "us-east-1",
        aws_access_key_id='AKIAIMWTUE6J5LGNZBMA',
        aws_secret_access_key='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')

    q = conn.get_queue('twitter-queue')
    queuecount = q.count()
    print "Queue count = " + str(queuecount)

    #_____ Connect to MongoDB _____
    client = MongoClient()
    client = MongoClient('localhost', 27017)
    db = client['myapp']


######## Find Sentiment Function ########
def findsentiment(tweet):
    splitTweet = tweet.split()
    sentiment = 0.0
    for word in splitTweet:
        if TERMS.has_key(word):
            sentiment = sentiment + float(TERMS[word])
    return sentiment


######## Parse Tweet Function ########
def parseTweet(tweet):
    if tweet.has_key('created_at'):
        createdat = tweet['created_at']
        hourint = int(createdat[11:13])
        parseaddic['hour'] = str(hourint)

        # _____ Retweets _____
    parseaddic['toptweets'] = {}
    if tweet.has_key('retweeted_status'):
        retweetcount = tweet['retweeted_status']['retweet_count']
        retweetscreenname = tweet['retweeted_status']['user']['screen_name'].encode('utf-8', errors='ignore')
        retweetname = tweet['retweeted_status']['user']['name'].encode('utf-8', errors='ignore')
        retweettext = tweet['retweeted_status']['text'].encode('utf-8', errors='ignore')
        retweetdic = {}
        retweetdic['retweetcount'] = retweetcount
        retweetdic['retweetscreenname'] = retweetscreenname
        retweetdic['retweetname'] = retweetname
        retweetdic['retweettext'] = retweettext
        retweetdic['retweetsentiment'] = findsentiment(retweettext)
        parseaddic['toptweets'] = retweetdic


    #_____ Text Sentiment _____
    if tweet.has_key('text'):
        text = tweet['text'].encode('utf-8', errors='ignore')
        parseaddic['text'] = text
        sentiment = findsentiment(text)
        parseaddic['sentimentscore'] = sentiment
        parseaddic['positivesentiment'] = 0
        parseaddic['negativesentiment'] = 0
        parseaddic['neutralsentiment'] = 0

        if sentiment > 0:
            parseaddic['positivesentiment'] = 1
        elif sentiment < 0:
            parseaddic['negativesentiment'] = 1
        elif sentiment == 0:
            parseaddic['neutralsentiment'] = 1

    #_____ Hashtags _____
    if tweet.has_key('entities'):
        res1 = tweet['entities']
        taglist = res1['hashtags']
        hashtaglist = []
        for tagiterm in taglist:
            hashtaglist.append(tagiterm['text'])
        parseaddic['hashtags'] = hashtaglist


######## Analyze Tweet Function ########
def analyzeTweet(tweetdic):
    text = tweetdic['text']
    text = text.lower()

    if not aggregatedic.has_key(keyword):
        valuedic = {'totaltweets': 0,
                    'positivesentiment': 0, 'negativesentiment': 0,'neutralsentiment':0, 'hashtags': {}, 'toptweets': {},
                    'totaltweets': 0, 'hourlyaggregate': {
        '0': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '1': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '2': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '3': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '4': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '5': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '6': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '7': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '8': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '9': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '10': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '11': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '12': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '13': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '14': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '15': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '16': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '17': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '18': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '19': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '20': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '21': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '22': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0},
        '23': {'totaltweets': 0, 'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0}
        }}

        aggregatedic[keyword] = valuedic

        # _____ Counts _____
    valuedic = aggregatedic[keyword]
    valuedic['totaltweets'] += 1
    valuedic['positivesentiment'] += tweetdic['positivesentiment']
    valuedic['negativesentiment'] += tweetdic['negativesentiment']
    valuedic['neutralsentiment'] += tweetdic['neutralsentiment']

    #_____ Hourly Aggregate _____
    hour = tweetdic['hour']
    valuedic['hourlyaggregate'][hour]['positivesentiment'] += tweetdic['positivesentiment']
    valuedic['hourlyaggregate'][hour]['negativesentiment'] += tweetdic['negativesentiment']
    valuedic['hourlyaggregate'][hour]['neutralsentiment'] += tweetdic['neutralsentiment']
    valuedic['hourlyaggregate'][hour]['positivesentiment'] += 1

    #_____ Top Hashtags _____
    tagsdic = valuedic['hashtags']
    for tag in tweetdic['hashtags']:
        if tagsdic.has_key(tag):
            tagsdic[tag] += 1
        else:
            tagsdic[tag] = 1

    #_____ Top Tweets _____
    if tweetdic.has_key('toptweets'):
        if tweetdic['toptweets'].has_key('retweetscreenname'):
            toptweetsdic = valuedic['toptweets']
            retweetkey = tweetdic['toptweets']['retweetscreenname']

            if toptweetsdic.has_key(retweetkey):
                toptweetsdic[retweetkey]['retweetcount'] = tweetdic['toptweets']['retweetcount']
            else:
                toptweetsdic[retweetkey] = tweetdic['toptweets']

    #_____ Aggregate _____
    aggregatedic[keyword] = valuedic


######## Post Processing Function ########
def postProcessing():
    print aggregatedic
    valuedic = aggregatedic[keyword]

    # _____ Top 10 Hashtags _____
    keysdic = valuedic['hashtags']
    sortedkeysdic = OrderedDict(sorted(keysdic.items(), key=lambda x: x[1], reverse=True))
    tophashtagsdic = {}
    i = 0
    for item in sortedkeysdic:
        if i > 9:
            break
        i = i + 1
        tophashtagsdic[item] = keysdic[item]

    valuedic['hashtags'] = tophashtagsdic

    #_____ Total Retweets & Top 10 Tweets _____
    toptweetsdic = valuedic['toptweets']
    for key in toptweetsdic:
        valuedic['totaltweets'] += toptweetsdic[key]['retweetcount']

    sortednames = sorted(toptweetsdic, key=lambda x: toptweetsdic[x]['retweetcount'], reverse=True)
    sortedtoptweetsdic = OrderedDict()
    i = 0
    for k in sortednames:
        if i > 99:
            break
        i = i + 1
        sortedtoptweetsdic[k] = toptweetsdic[k]

    valuedic['toptweets'] = sortedtoptweetsdic
    #print valuedic['toptweets']

    #_____ Create Key for MongoDB document _____
    valuedic['_id'] = str(date.today()) + "/" + keyword
    valuedic['metadata'] = {'date': str(date.today()), 'key': keyword}

    #_____ Insert into MongoDB _____
    print valuedic
    print "Inserting data into MongoDB"
    postid = db.myapp_micollection.insert(valuedic)


######## Main Function: Consigure consumeCount ########
def main():
    print "Setting ip consumer..."
    setupConsumer()
    print "Completed consumer setup..."

    # _____ enter no. of tweets to consume _____
    consumeCount = 800

    print "Consuming " + str(consumeCount) + " feed..."

    consumeCount = consumeCount / 10  #get 10 in each batch
    for i in range(consumeCount):
        rs = q.get_messages(10)  #gets max 10 msgs at a time

        if len(rs) > 0:
            for m in rs:
                post = m.get_body()
                deserializedpost = cPickle.loads(str(post))
                postdic = json.loads(deserializedpost)

                parseTweet(postdic)
                analyzeTweet(parseaddic)

            conn.delete_message_batch(q, rs)

    queuecount = q.count()
    print "Remaining Queue count= " + str(queuecount)
    print "Completed Consuming..."
    print "Starting post processing..."
    postProcessing()
    print "Completed post processing..."
    print "Done!"

######## Entry Point #####
if __name__ == '__main__':
    main()































