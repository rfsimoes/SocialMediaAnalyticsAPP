from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import StreamListener
from tweepy import Stream
from boto.sqs.message import Message
import cPickle

consumer_key = '<enter key>'
consumer_secret = '<enter secret>'
access_token = '<enter access token>'
access_token_secret = '<enter access secret>'

import boto.sqs

conn = boto.sqs.connect_to_region(
    'us-east-1',
    aws_access_key_id='<enter key>',
    aws_secret_access_key='<enter secret>')

q = conn.get_all_queues(prefix='arsh-queue')


class StdOutListener(StreamListener):
    def on_data(self, data):
        # print data
        msg = cPickle.dumps(data)
        m = Message()
        m.set_body(msg)
        status = q[0].write(m)
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    keywords = ['India']
    I = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, I)
    stream.filter(track=keywords)

