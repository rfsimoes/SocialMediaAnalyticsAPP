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


    while True:
        # Ask user what he wants to do
        while True:
            try:
                choice = int(raw_input("Please choose an option:\n[1] Download a new set of tweets\n[2] Search existing data\n>>> "))
            except ValueError:
                print "That's not a number!"
            else:
                if 1 <= choice <= 2:
                    break
                else:
                    print 'Invalid option. Try again!'

        # Ask user which key he wants to search
        user_input = raw_input("Please enter a key to search [enter to exit]: ")

        # Exit program
        if user_input == "":
            print "Exiting program...\n"
            exit(0)

        # Define search key
        keyy = user_input

        # If user choose to download a new set of tweets
        if choice == 1:
            download_tweets(keyy)
        # If user choose to search existing data
        else:
            search_data(keyy)


# This function invokes TweetsListener and TweetsConsumer in order to download a new set of tweets
def download_tweets(key):
    #TweetsListener
    print "----------------------------------------------"
    print "          Starting Listener..."
    print "----------------------------------------------"
    try:
        thread.start_new_thread(Listener.run, (key,))
    except:
        print "Error thread Listener"

    #TweetsConsumer
    print "----------------------------------------------"
    print "          Starting Consumer..."
    print "----------------------------------------------"
    Consumer.main(key)
    print "----------------------------------------------"
    print "          Consumer Completed!"
    print "----------------------------------------------"


# This function uses CloudSearch to search data based on a keyword
def search_data(key):
    print "----------------------------------------------"
    print "          Starting CloudSearch..."
    print "----------------------------------------------"

    # Connect to CloudSearch
    print "Connecting to CloudSearch..."
    connCS = boto.connect_cloudsearch2(aws_access_key_id='AKIAIXQMLV2XPFQ2NFRQ',
                                       aws_secret_access_key='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')
    print "Connected!\n"

    # Search domain
    print "Searching for domain..."
    domain = connCS.lookup('twitter-app')
    print "Domain founded: ", domain.name


    # Searching results
    print "Searching for '" + key + "'...\n"
    search_service = domain.get_search_service()
    # Search for key and sort results by date
    results = search_service.search(q=key, sort=['id_stat asc'])
    if results.hits == 0:
        print "No results founded with search key '" + key + "'"
        exit(0)
    print "We found " + str(results.hits) + " results!"

    # Show dates of results to users
    for i in range(results.hits):
        result_date = map(lambda x: x["fields"]["metadata"][10:20], results)[i]
        print "[" + str(i) + "] " + str(result_date)

    # Ask users which result they want to see
    while True:
        try:
            input_number = int(raw_input('Pick a number in range 0-' + str(results.hits - 1) + ' >>> '))
        except ValueError:
            print "That's not a number!"
        else:
            if 0 <= input_number <= results.hits:
                break
            else:
                print 'Out of range. Try again!'

    statistics(results, input_number)


# This function calculates and shows statistics
def statistics(results, index):
    # Calculate statistics
    totaltweets = map(lambda x: x["fields"]["total_tweets"], results)[index]
    positivesentiment = map(lambda x: x["fields"]["positive_sentiment"], results)[index]
    negativesentiment = map(lambda x: x["fields"]["negative_sentiment"], results)[index]
    neutralsentiment = map(lambda x: x["fields"]["neutral_sentiment"], results)[index]

    #totaltweets = results['total_tweets']
    #positivesentiment = results['positive_sentiment']
    #negativesentiment = results['negative_sentiment']
    #neutralsentiment = results['neutral_sentiment']

    pospercent = float(positivesentiment) * 100 / float(totaltweets)
    negpercent = float(negativesentiment) * 100 / float(totaltweets)
    neupercent = float(neutralsentiment) * 100 / float(totaltweets)

    #hashtags = results['hashtags']
    #hashtags = OrderedDict(sorted(hashtags.items(), key=lambda x: x[1], reverse=True))

    #toptweetsdic = results['top_tweets']
    #sortednames = sorted(toptweetsdic, key=lambda x: toptweetsdic[x]['retweetcount'], reverse=True)
    #sortedtoptweetsdic = OrderedDict()
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

    #hourlyaggregate = results['hourly_aggregate']
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

    # Show statistics to users
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


if __name__ == '__main__':
    home()