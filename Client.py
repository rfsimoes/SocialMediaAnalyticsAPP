import json
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

def home():
    #results = db.myapp_micollection.find({'metadata.key': 'india', 'metadata.date': today})
    # results=db.myapp_micollection.find({'metadata.key':'query','metadata.date':'2013-06-30'})
    keyy = 'india'
    hash = today + '/' + keyy
    range = "{'date': '" + today + "', 'key': '" + keyy + "'}"
    results = table.get_item(hash_key=hash,range_key=range)

    #print 'Results: ' ,results


    totaltweets = results['total_tweets']
    positivesentiment = results['positive_sentiment']
    negativesentiment = results['negative_sentiment']
    neutralsentiment = results['neutral_sentiment']

    pospercent = float(positivesentiment) * 100 / float(totaltweets)
    negpercent = float(negativesentiment) * 100 / float(totaltweets)
    neupercent = float(neutralsentiment) * 100 / float(totaltweets)

    hashtags = results['hashtags']
    #hashtags = OrderedDict(sorted(hashtags.items(), key=lambda x: x[1], reverse=True))

    toptweetsdic = results['top_tweets']
    #sortednames = sorted(toptweetsdic, key=lambda x: toptweetsdic[x]['retweetcount'], reverse=True)
    sortedtoptweetsdic = OrderedDict()
    """
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
    """

    hourlyaggregate = results['hourly_aggregate']
    """
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
    """
    print '-----------------------------'
    print '-----TWITTER STATISTICS------'
    print '-----------------------------'
    print 'Total tweets: ', totaltweets
    print 'Positive sentiment tweets: ', positivesentiment
    print 'Neutral sentiment tweets: ', neutralsentiment
    print 'Negative sentiment tweets: ', negativesentiment
    print 'Positive percentage: ',pospercent
    print 'Neutral percentage: ',neupercent
    print 'Negative percentage: ',negpercent
    print
    #print 'Hashtags: ',hashtags
    """return render_to_response('index.html', {'totaltweets': totaltweets,
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
                                             'toptweets': toptweets})"""

if __name__ == '__main__':
    home()