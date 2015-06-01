import cPickle
from datetime import date
import json
from collections import OrderedDict

import boto.sqs
from boto.sqs.message import Message
import boto.dynamodb


parseaddic = {}
aggregatedic = {}
TERMS = {}
global conn
global connDB
global q
global client
global db
global keyword
global hashDB
global rangeDB


# This method makes a connection to DynamoDB
def connect():
    print 'Connecting to DynamoDB...'
    connDB = boto.dynamodb.connect_to_region('us-east-1',
                                             aws_access_key_id='AKIAIXQMLV2XPFQ2NFRQ',
                                             aws_secret_access_key='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')
    print 'Connected!\n'
    return connDB


######## Setup Consumer Function ########
def setupConsumer(key):
    global conn
    global connDB
    global q
    global client
    global db
    global keyword
    global table

    # _____ Initialize keyword list _____
    keyword = key

    # _____ Load Sentiments Dictionary _____
    sent_file = open('AFINN-111.txt')
    sent_lines = sent_file.readlines()
    for line in sent_lines:
        s = line.split(".")
        TERMS[s[0].strip()] = int(s[1].strip())
    sent_file.close()

    # _____ Connect to SQS Queue _____
    print 'Connecting to SQS...'
    conn = boto.sqs.connect_to_region(
        "us-east-1",
        aws_access_key_id='AKIAIXQMLV2XPFQ2NFRQ',
        aws_secret_access_key='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')
    print 'Connected!\n'

    q = conn.get_queue('twitter-queue')
    queuecount = q.count()
    print "Queue count = " + str(queuecount) + "\n"

    # _____ Connect to DynamoDB _____
    connDB = connect()
    table = connDB.get_table('twitter_stats_table')


######## Find Sentiment Function ########
# This function computes the sentiment of the parsed tweet
def findsentiment(tweet):
    splitTweet = tweet.split()
    sentiment = 0.0
    for word in splitTweet:
        if TERMS.has_key(word):
            sentiment = sentiment + float(TERMS[word])
    return sentiment


######## Parse Tweet Function ########
# This function parses the field in the tweet object (JSON object)
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


    # _____ Text Sentiment _____
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

    # _____ Hashtags _____
    if tweet.has_key('entities'):
        res1 = tweet['entities']
        taglist = res1['hashtags']
        hashtaglist = []
        for tagiterm in taglist:
            hashtaglist.append(tagiterm['text'])
        parseaddic['hashtags'] = hashtaglist


######## Analyze Tweet Function ########
# This function analyzes and aggregates the results
def analyzeTweet(tweetdic):
    text = tweetdic['text']
    text = text.lower()

    if not aggregatedic.has_key(keyword):
        valuedic = {'totaltweets': 0,
                    'positivesentiment': 0, 'negativesentiment': 0, 'neutralsentiment': 0, 'hashtags': {},
                    'toptweets': {},
                    'totalretweets': 0, 'hourlyaggregate': {
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

    # _____ Hourly Aggregate _____
    hour = tweetdic['hour']
    valuedic['hourlyaggregate'][hour]['positivesentiment'] += tweetdic['positivesentiment']
    valuedic['hourlyaggregate'][hour]['negativesentiment'] += tweetdic['negativesentiment']
    valuedic['hourlyaggregate'][hour]['neutralsentiment'] += tweetdic['neutralsentiment']
    valuedic['hourlyaggregate'][hour]['positivesentiment'] += 1

    # _____ Top Hashtags _____
    tagsdic = valuedic['hashtags']
    for tag in tweetdic['hashtags']:
        if tagsdic.has_key(tag):
            tagsdic[tag] += 1
        else:
            tagsdic[tag] = 1

    # _____ Top Tweets _____
    if tweetdic.has_key('toptweets'):
        if tweetdic['toptweets'].has_key('retweetscreenname'):
            toptweetsdic = valuedic['toptweets']
            retweetkey = tweetdic['toptweets']['retweetscreenname']

            if toptweetsdic.has_key(retweetkey):
                toptweetsdic[retweetkey]['retweetcount'] = tweetdic['toptweets']['retweetcount']
            else:
                toptweetsdic[retweetkey] = tweetdic['toptweets']

    # _____ Aggregate _____
    aggregatedic[keyword] = valuedic


######## Post Processing Function ########
def postProcessing():
    # print aggregatedic
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

    # _____ Total Retweets & Top 10 Tweets _____
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
    # print valuedic['toptweets']

    # _____ Create Key for DynamoDB _____
    valuedic['_id'] = str(date.today()) + "/" + keyword
    valuedic['metadata'] = {'date': str(date.today()), 'key': keyword}

    # _____ Insert into DynamoDB _____
    addItem(valuedic)

    # Update CloudSearch
    update_cloudsearch()


def saveToFile(id_stat, item_data):
    string = ''
    string = string + id_stat + '\n'
    for key, value in item_data.items():
        string = string + key + ' = ' + value + '\n'

    f = open("file", 'w')
    f.write("file")
    f.close()


def uploadToGlacier(id_stat):
    glacier_connection = boto.connect_glacier(aws_access_key_id='AKIAIXQMLV2XPFQ2NFRQ',
                                              aws_secret_access_key='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')
    vault = glacier_connection.get_vault("myvault")
    archive_id = vault.upload_archive('file')


# This function adds an item to DynamoDB
def addItem(valuedic):
    print 'Inserting data on DynamoDB...'
    # Define statistics variables
    id_stat = valuedic['_id']
    total_tweets = valuedic['totaltweets']
    positive_tweets = valuedic['positivesentiment']
    neutral_tweets = valuedic['neutralsentiment']
    negative_tweets = valuedic['negativesentiment']
    hashtags = valuedic['hashtags']
    top_tweets = valuedic['toptweets']
    total_retweets = valuedic['totalretweets']
    metadata = valuedic['metadata']
    hourly_aggregate = valuedic['hourlyaggregate']
    table = connDB.get_table('twitter_stats_table')
    item_data = {
        'total_tweets': str(total_tweets),
        'positive_sentiment': str(positive_tweets),
        'neutral_sentiment': str(neutral_tweets),
        'negative_sentiment': str(negative_tweets),
        'hashtags': str(hashtags),
        'top_tweets': str(top_tweets),
        'total_retweets': str(total_retweets),
        'hourly_aggregate': str(hourly_aggregate)
    }
    print "Saving data to 'file'"
    saveToFile(id_stat, item_data)
    print "Uploading 'file' to Glacier"
    uploadToGlacier(id_stat)
    print "Finished uploading to Glacier"

    global hashDB
    global rangeDB
    hashDB = str(id_stat)
    rangeDB = str(metadata)

    item = table.new_item(
        hash_key=hashDB,
        range_key=rangeDB,
        attrs=item_data
    )
    item.put()
    print 'Data was successfully inserted!'


def update_cloudsearch():
    # Get the result from table
    results = table.get_item(hash_key=hashDB, range_key=rangeDB)

    # Connect to CloudSearch
    print "Connecting to CloudSearch..."
    connCS = boto.connect_cloudsearch2(aws_access_key_id='AKIAIXQMLV2XPFQ2NFRQ',
                                       aws_secret_access_key='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')
    print "Connected!\n"

    # Search domain
    print "Searching for domain..."
    domain = connCS.lookup('twitter-app')
    print "Domain founded: ", domain.name

    print "Updating CloudSearch..."
    doc_service = domain.get_document_service()
    doc_service.add(results.get('id_stat'), results)
    doc_service.commit()
    print "Updated!"


# This function creates a new DynamoDB database
def create_database():
    connDB = connect()
    schemaTable = connDB.create_schema(
        hash_key_name='id_stat',
        hash_key_proto_value=str,
        range_key_name='metadata',
        range_key_proto_value=str
    )
    table = connDB.create_table(
        name='twitter_stats_table',
        schema=schemaTable,
        read_units=1,
        write_units=1
    )


######## Main Function: Consigure consumeCount ########
def main(keyy):
    print "Setting up consumer...\n"
    setupConsumer(keyy)
    print "Completed consumer setup..."

    # _____ enter no. of tweets to consume _____
    consumeCount = 800

    print "Consuming " + str(consumeCount) + " feed..."

    consumeCount = consumeCount / 10  # get 10 in each batch
    for i in range(consumeCount):
        rs = q.get_messages(10)  # gets max 10 msgs at a time

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
            print "Completed Consuming...\n"
            print "Starting post processing..."
            postProcessing()
            print "Completed post processing..."
            print "Done!\n"

######## Entry Point #####
if __name__ == '__main__':
    key = 'benfica'
    main(key)































