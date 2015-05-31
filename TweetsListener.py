from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import StreamListener
from tweepy import Stream
from boto.sqs.message import Message
import cPickle
import logging
import boto.sqs
import sys
import thread


# Twitter API credentials
consumer_key = 'WIGpzXr5ruz2pac5r93bWbgPX'
consumer_secret = 'zxnJmq0OgeweZtcD4veZdAXJqNqTjVjbnaMiQVtWrw3mPm8TnO'
access_token = '433342420-XqBbNHsEQiK9ccVzyOJeN1cjgCrRqSlB3S8bKoaI'
access_token_secret = '2EW7uTlqNGACpnMA8FVFiw7SWlUKcGUthUkvG71GEVxDa'
maxCount=5
actualCount=0

# Connect to SQS
print 'Connecting to SQS...'
conn = boto.sqs.connect_to_region(
    'us-east-1',
    aws_access_key_id='AKIAIXQMLV2XPFQ2NFRQ',
    aws_secret_access_key='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')
print 'Connected!\n'


# Get all queues with 'twitter-queue' as a prefix
print 'Getting the available queues...'
q = conn.get_all_queues(prefix='twitter-queue')

# If there is no queues with the prefix
if(q == []):
    print 'No queues available. Creating a queue...'
    # Create a queue named 'twitter-queue'
    q = conn.create_queue('twitter-queue')
    print 'Queue created!\n'
# If there is a queue with the prefix
else:
    # Associate the queue
    q = q[0]
    print 'Queue retrieved!\n'



def run(keyy):
    logging.captureWarnings(True)
    # List of keywords
    keywords = list()
    keywords.append(keyy)
    I = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, I)
    stream.filter(track=keywords)



class StdOutListener(StreamListener):

    # This method is called whenever a new tweet related to the keyword (or list of keywords) is received
    def on_data(self, data):
        global maxCount
        global actualCount
        if actualCount==maxCount:
            print "----------------------------------------------"
            print "          Listener Completed!"
            print "----------------------------------------------"
            thread.exit()
        print 'New tweet received'
        msg = cPickle.dumps(data)
        m = Message()
        m.set_body(msg)
        status = q.write(m)
        print "Tweet was sended to the queue '" + str(q.name) + "'\n"
        actualCount=actualCount+1
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    logging.captureWarnings(True)
    # List of keywords
    keywords = ['India']
    I = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, I)
    stream.filter(track=keywords)

