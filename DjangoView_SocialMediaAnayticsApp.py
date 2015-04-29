from django.shortcuts import render_to_response
import json
from pymongo import MongoClient
from collections import OrderedDict
from datetime import date, timedelta

today = str(date.today())

client = MongoClient()
cliet = MongoClient('localhost', 27017)
db = cliet['myapp']


def home(request):
    results = db.myapp_micollection.find({'metadata.key': 'india', 'metadata.date': today})
    # results=db.myapp_micollection.find({'metadata.key':'query','metadata.date':'2013-06-30'})

    for postdic in results:
        totaltweets = postdic['totaltweets']
        positivesentiment = postdic['positivesentiment']
        negativesentiment = postdic['negativesentiment']
        neutralsentiment = postdic['neutralsentiment']

        pospercent = positivesentiment * 100 / totaltweets
        negpercent = negativesentiment * 100 / totaltweets
        neupercent = neutralsentiment * 100 / totaltweets

        hashtags = postdic['hashtags']
        hashtags = OrderedDict(sorted(hashtags.items(), key=lambda x: x[1], reverse=True))

        toptweetsdic = postdic['toptweets']
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

        hourlyaggregate = postdic['hourlyaggregate']

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
                                             'positivesentiment': positivesentiment, 'negativesentiment':
        negative, 'neutralsentiment': neutralsentiment, 'pospercent': pospercent,
                                             'negpercent': negpercent, 'neupercent': neupercent, 'hashtags': hashtags,
                                             'hourlyaggregate': hourlyaggregate, 'total': total, 'positive': positive,
                                             'negative': negative, 'neutral': neutral, 'toptweets': toptweets})
