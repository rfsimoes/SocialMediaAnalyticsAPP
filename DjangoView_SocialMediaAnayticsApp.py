from django.shortcuts import render_to_response
import json
from pymongo import MongoClient
from collections import OrderedDict
from datetime import date, timedelta
import boto.dynamodb


today = str(date.today())


#Conexao
def connect():
    conn = boto.dynamodb.connect_to_region('us-east-1',
    aws_access_key_id='AKIAIMWTUE6J5LGNZBMA',
    aws_secret_access_key='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')
    return conn

# Connect to DynamoDB
connDB = connect()
table = connDB.get_table('twitter_stats_table')

def home(request):
    #results = db.myapp_micollection.find({'metadata.key': 'india', 'metadata.date': today})
    # results=db.myapp_micollection.find({'metadata.key':'query','metadata.date':'2013-06-30'})
    key = 'india'
    hash = today + '/' + key
    range = "{'date': '" + today + "', 'key': '" + key + "'}"
    results = table.get_item(hash_key=hash,range_key=range)

    for postdic in results:
        totaltweets = postdic['total_tweets']
        positivesentiment = postdic['positive_sentiment']
        negativesentiment = postdic['negative_sentiment']
        neutralsentiment = postdic['neutral_sentiment']

        pospercent = positivesentiment * 100 / totaltweets
        negpercent = negativesentiment * 100 / totaltweets
        neupercent = neutralsentiment * 100 / totaltweets

        hashtags = postdic['hashtags']
        hashtags = OrderedDict(sorted(hashtags.items(), key=lambda x: x[1], reverse=True))

        toptweetsdic = postdic['top_tweets']
        sortednames = sorted(toptweetsdic, key=lambda x: toptweetsdic[x]['retweetcount'], reverse=True)
        sortedtoptweetsdic = OrderedDict()

        i = 0
        for k in sortednames:
            if i > 4:
                break
            i = i + 1
            sortedtoptweetsdic[k] = toptweetsdic[k]

        toptweets = OrderedDict()
        for key in sortedtoptweetsdic:
            t = []
            t.append(sortedtoptweetsdic[key]['retweetscreenname'])
            t.append(sortedtoptweetsdic[key]['retweetname'])
            t.append(sortedtoptweetsdic[key]['retweettext'])
            t.append(sortedtoptweetsdic[key]['retweetcount'])
            t.append(sortedtoptweetsdic[key]['retweetsentiment'])
            t.append(sortedtoptweetsdic[key]['retweetimage'])
            toptweets[key] = t

        hourlyaggregate = postdic['hourly_aggregate']

        total = {}
        positive = {}
        negative = {}
        neutral = {}
        hi = {}
        for key in hourlyaggregate:
            hi[int(key)] = hourlyaggregate[key]

        hourlyaggregate = OrderedDict(sorted(hi.items()))

        for entry in hourlyaggregate:
            total[entry] = hourlyaggregate[entry]['totaltweets']
            positive[entry] = hourlyaggregate[entry]['positivesentiment']
            negative[entry] = hourlyaggregate[entry]['negativesentiment']
            neutral[entry] = hourlyaggregate[entry]['neutralsentiment']

    return render_to_response('index.html', {'totaltweets': totaltweets,
                                             'positivesentiment': positivesentiment,
                                             'negativesentiment': negativesentiment,
                                             'neutralsentiment': neutralsentiment,
                                             'pospercent': pospercent,
                                             'negpercent': negpercent,
                                             'neupercent': neupercent,
                                             'hashtags': hashtags,
                                             'hourlyaggregate': hourlyaggregate,
                                             'total': total,
                                             'positive': positive,
                                             'negative': negative,
                                             'neutral': neutral,
                                             'toptweets': toptweets})
