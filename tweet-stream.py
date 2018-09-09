from threading import Thread
from queue import Queue
from requests_oauthlib import OAuth1

import requests
import time
import json
import ssl
import sys

from config import Config

class _StreamFilterThread(Thread):
  """Filter the Twitter stream and store tweets matching the given criteria.

  Acts as a "producer" to the "consumer" _TweetConsumerThread. Extends base Thread class. 
  Shouldn't be instantiated outside of the module. Use TweetStrainer.

  Currently only the "track" parameter for the filter has been implemented (i.e. a text
  search through some fields of the tweet object). Since reading a streaming response can
  result in blocking calls, this thread yields after reading a specified number of tweets
  and when keep-alive newlines are detected to give the consumer a chance to clear the queue.

  Instance members:
  queue -- the shared queue with the producer, holds tweets as byte strings to be processed.
  verbose -- if True, status messages will be printed to the console.
  track -- list of terms to filter for
  """

  def __init__(self, queue, verbose=True, **options):
    super().__init__()
    self.queue = queue
    self.verbose = verbose
    self.new_session()
    self.track = options.get("track")
    if self.track is not None and not isinstance(self.track, list):
      self.track = [self.track]
    self.url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    self.running = False
    
    # Below directly from tweepy/streaming.py: https://github.com/tweepy/tweepy
    # values according to
    # https://dev.twitter.com/docs/streaming-apis/connecting#Reconnecting
    self._retry_time_start = options.get("retry_time", 5.0)
    self._retry_420_start = options.get("retry_420", 60.0)
    self._retry_time_cap = options.get("retry_time_cap", 320.0)
    self._snooze_time_step = options.get("snooze_time", 0.25)
    self._snooze_time_cap = options.get("snooze_time_cap", 16)
    self._timeout = options.get("timeout", 300.0)
    self._retry_count = options.get("retry_count")

  def _parse_stream(self, resp):
    """ Parse incoming tweets from the streaming response and put them in a thread queue.

    Detects keep-alive newlines sent every ~30 seconds. If a keep-alive is detected,
    no tweets have been received for a while so the thread sleeps for a brief time to yield
    to the consumer. 
    
    Note that no processing is done to the tweets at this point. The byte string received
    is added directly to the queue. JSON decoding is done by the consumer thread.   

    Parameters:
    resp -- a requests.Response instance as an open stream.
    """
    verb = self.verbose
    tweets_read = 0
    last_keep_alive = time.perf_counter()
    while self.running and not resp.raw.closed:
      for next_tweet in resp.iter_lines():
        if next_tweet and next_tweet != b'\n':
          # received tweet, process it
          self.queue.put(next_tweet)
          tweets_read += 1
          if tweets_read > 10:
            # give the consumer a chance to catch up and clear the queue
            if verb: print("<< READER >> loaded 10 tweets...sleeping....\n")
            time.sleep(1)
            tweets_read = 0
        elif next_tweet == b'':
          # got a newline keep-alive, count it and sleep so consumer can catch up
          # sometimes multiple newlines are sent rapidly, make sure this is a "real" keep-alive
          now = time.perf_counter()
          if now - last_keep_alive > 5:
            last_keep_alive = now
            if verb: print("<< READER >> keep alive detected; sleeping to yield.\n")
            time.sleep(1)

    if verb: print("stopped running or connection closed") 
    self.running = False   

  def run(self):
    """ Begin monitoring the Twitter stream.

    Opens a streaming connection to the Twitter filter stream.  Keeps running until interrupted
    in some way (keyboard interrupt, lost connection, timeout, etc). 
    The logic of the error handling is borrowed almost directly from the Tweepy streaming logic: https://github.com/tweepy/tweepy
    """ 
    verb = self.verbose
    self.running = True
    self.session.params = {}
    self.session.params["track"] = u','.join(self.track)

    exc_info = None
    retry_time = self._retry_time_start
    retry_420_time = self._retry_420_start
    snooze_time = self._snooze_time_step
    while self.running:
      try:
        resp = self.session.post(self.url, stream=True, timeout=30)
        # 420: Rate Limited; connected too frequently. 
        # Wait 1 minute, back-off exponentially (x2) for each subsequent 420 status
        if resp.status_code == 420:
          if verb: 
            print(">> Status code 420: Rate Limited <<")
            print(">> Sleeping for {} before attempting to connect again <<".format(retry_420_time))
          time.sleep(retry_420_time)
          retry_420_time *= 2
        # 503: Service Unavailable
        # Wait 0.25s, back off linearly (step=0.25s) up to 16s
        elif resp.status_code == 503:
          if verb: 
            print(">> Status code 503: Service Unavailable <<")
            print(">> Sleeping for {} before attempting to connect again <<".format(snooze_time))
          time.sleep(snooze_time)
          snooze_time += self._snooze_time_step
          snooze_time = min(snooze_time, self._snooze_time_cap)
        # Other unrecoverable non-200 status codes, stop running
        elif resp.status_code != 200:
          if verb: 
            print('>> Status code {} <<'.format(resp.status_code))
            print(">> Stopping... <<")
          self.running = False
        # 200: Success! 
        else:
          if verb: print(">> Connected to Twitter stream:", self.track)
          self._parse_stream(resp)
      except (requests.exceptions.Timeout, ssl.SSLError) as e:
        # if its an SSL error and isn't related to timing out, treat like any other exception
        if isinstance(e, ssl.SSLError):
          if not (e.args and "timed out" in str(e.args[0])):
            exc_info = sys.exc_info()
            break
        # otherwise, its a timeout, wait appropriate time
        time.sleep(self.snooze_time)
        self.snooze_time += self._snooze_time_step
        self.snooze_time = min(self.snooze_time, self._snooze_time_cap)
      except Exception as e:
        exc_info = sys.exc_info()
        break
    
    # cleanup
    self.running = False
    if resp: resp.close()
    self.new_session()

    # Re-raise any exceptions not addressed
    if exc_info is not None:
      raise exc_info[0]
    
  def new_session(self):
    """ Create a new session for the thread and assign the proper OAuth1 credentials. """
    self.session = requests.Session()
    self.session.params = None
    self.session.auth = OAuth1(
      Config.client_id, 
      client_secret=Config.client_secret,
      resource_owner_key=Config.resource_owner_key,
      resource_owner_secret=Config.resource_owner_secret)


class _TweetConsumerThread(Thread):
  """Process tweets parsed by _StreamFilterThread.

  Acts as a "consumer" to the "producer" _StreamFilterThread. Extends base Thread class.
  This class shouldn't be instantiated directly outside the module. Use TweetStrainer.

  Instance members:
  queue -- the shared queue with the producer, holds tweets as byte strings to be processed.
  """
  
  def __init__(self, queue, verbose=True):
    super().__init__()
    self.queue = queue
    self.verbose = verbose

  def run(self):
    """JSON decode tweet byte strings from the queue.

    Currently select fields are simply printed, but other data processing could be
    accomplished here instead.
    """
    while True:
      tweet_obj = self.queue.get()
      tweet = None
      try:
        tweet = json.loads(tweet_obj)
      except json.JSONDecodeError as err:
        print("Found an object not properly JSON formatted...")
      else:
        if self.verbose: self.print_tweet(tweet)
      finally:
        self.queue.task_done()
  
  def print_tweet(self, tweet):
    text = tweet["text"]
    user = tweet["user"]["screen_name"]
    if "retweeted_status" in tweet:
      re = tweet["retweeted_status"]
      print(">> RETWEET <<")
      user = tweet["user"]["screen_name"] + " RT'd " + re["user"]["screen_name"]
      text = re["text"]
    if tweet['truncated']:
      print('>> LONG TWEET <<')
      text = tweet['extended_tweet']['full_text']
    print(user, ">> ", end='')
    print(text)
    print(tweet['user']['location'])
    print()
      

class TweetStrainer(object):
  """Connect to the Twitter stream and track the given terms.

  Wrapper class for the producer/consumer pair of classes _StreamFilterThread and 
  _TweetConsumerThread. 
  
  Instance members:
  track -- list of string terms to track
  """
  def __init__(self, track):
    if isinstance(track, list):
      self.track = list(map(str, track))
    else:
      self.track = [str(track)]

    self._queue = Queue()
    self._threads = [
      _StreamFilterThread(self._queue, track=self.track),
      _TweetConsumerThread(self._queue)
    ]
      
  def run(self):
    """Run the producer/consumer threads, wait until they end."""
    for thread in self._threads:
      thread.start()

    for thread in self._threads:
      thread.join()


if __name__ == "__main__":
  ts = TweetStrainer("portland")
  ts.run()
