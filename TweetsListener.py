from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import StreamListener
from tweepy import Stream
from boto.sqs.message import Message
import cPickle
import logging

consumer_key = 'WIGpzXr5ruz2pac5r93bWbgPX'
consumer_secret = 'zxnJmq0OgeweZtcD4veZdAXJqNqTjVjbnaMiQVtWrw3mPm8TnO'
access_token = '433342420-XqBbNHsEQiK9ccVzyOJeN1cjgCrRqSlB3S8bKoaI'
access_token_secret = '2EW7uTlqNGACpnMA8FVFiw7SWlUKcGUthUkvG71GEVxDa'

import boto.sqs

conn = boto.sqs.connect_to_region(
    'us-east-1',
    aws_access_key_id='AKIAIMWTUE6J5LGNZBMA',
    aws_secret_access_key='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')

q = conn.get_all_queues(prefix='twitter-queue')

# There is no queues with this prefix
if(q == []):
    # Create queue
    q = conn.create_queue('twitter-queue')
# There is a queue
else:
    q = q[0]

class StdOutListener(StreamListener):
    def on_data(self, data):
        # print data
        msg = cPickle.dumps(data)
        m = Message()
        m.set_body(msg)
        status = q.write(m)
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    logging.captureWarnings(True)
    keywords = ['India']
    I = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, I)
    stream.filter(track=keywords)

