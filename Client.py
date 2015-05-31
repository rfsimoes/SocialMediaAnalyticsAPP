import json
from collections import OrderedDict
from datetime import date, timedelta
import boto.dynamodb
import TweetsListener as Listener
import TweetsConsumer as Consumer
import thread


today = str(date.today())


# This method makes a connection to DynamoDB
def connect():
    print 'Connecting to DynamoDB...'
    conn = boto.dynamodb.connect_to_region('us-east-1',
                                           aws_access_key_id='AKIAIXQMLV2XPFQ2NFRQ',
                                           aws_secret_access_key='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')
    print 'Connected!\n'
    return conn


# Connect to DynamoDB
connDB = connect()
table = connDB.get_table('twitter_stats_table')


# This is the main method
def home():
    print " _____________________________________________________________"
    print "|  _________________________________________________________  |"
    print "| |                                                         | |"
    print "| |         WELCOME TO SOCIAL MEDIA ANALYTICS APP           | |"
    print "| |_________________________________________________________| |"
    print "|_____________________________________________________________|\n"



    # results = db.myapp_micollection.find({'metadata.key': 'india', 'metadata.date': today})
    # results=db.myapp_micollection.find({'metadata.key':'query','metadata.date':'2013-06-30'})



    while True:
        # Ask user which key he wants to search
        user_input = raw_input("Please enter a key to search [enter to exit]: ")

        # Exit program
        if user_input == "":
            print "Exiting program...\n"
            exit(0)

        # Define search key
        keyy = user_input

        #TweetsListener
        print "----------------------------------------------"
        print "          Starting Listener..."
        print "----------------------------------------------"
        #Listener.run(keyy)
        try:
            thread.start_new_thread(Listener.run, (keyy,))
        except:
            print "Error thread Listener"

        #TweetsConsumer
        print "----------------------------------------------"
        print "          Starting Consumer..."
        print "----------------------------------------------"
        Consumer.main(keyy)
        print "----------------------------------------------"
        print "          Consumer Completed!"
        print "----------------------------------------------"

        # Define hash_key
        hash = today + '/' + keyy
        # Define range_key
        range = "{'date': '" + today + "', 'key': '" + keyy + "'}"

        # Check if hash_key exists
        if table.has_item(hash, range, True) == False:
            print "Key does not exist!\n"
        else:
            break

    # Get the result from table
    results = table.get_item(hash_key=hash, range_key=range)

    print 'Results: ', results

    print "----------------------------------------------"
    print "          Starting CloudSearch..."
    print "----------------------------------------------"

    # Connect to CloudSearch
    print "Connecting to CloudSearch..."
    connCS = boto.connect_cloudsearch2(aws_access_key_id='AKIAIXQMLV2XPFQ2NFRQ',
                                       aws_secret_access_key='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')
    print "Connected!\n"

    print "Searching for domain..."
    domain = connCS.lookup('twitter-app')
    print "Domain founded: ", domain
    print "Meter merdas la pra dentro"
    doc_service = domain.get_document_service()
    #for r in results:
    doc_service.add(results.get('id_stat'), results)
    result = doc_service.commit()
    print "Tudo la dentro ", result

    print "Searching for '" + keyy + "'...\n"
    search_service = domain.get_search_service()
    results = search_service.search(q=keyy)

    totaltweets = map(lambda x: x["fields"]["total_tweets"], results)[2]
    positivesentiment = map(lambda x: x["fields"]["positive_sentiment"], results)[2]
    negativesentiment = map(lambda x: x["fields"]["negative_sentiment"], results)[2]
    neutralsentiment = map(lambda x: x["fields"]["neutral_sentiment"], results)[2]


    #totaltweets = results['total_tweets']
    #positivesentiment = results['positive_sentiment']
    #negativesentiment = results['negative_sentiment']
    #neutralsentiment = results['neutral_sentiment']


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
    print 'Positive percentage: ', pospercent
    print 'Neutral percentage: ', neupercent
    print 'Negative percentage: ', negpercent
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