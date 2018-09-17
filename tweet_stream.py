from threading import Thread
from queue import Queue
from requests_oauthlib import OAuth1

import requests
import time
from datetime import datetime
import json
import ssl
import sys

from config import Config


class Tweet(object):
  """Container for information contained in a tweet object.

  This class does not represent a complete tweet; only some of the data has been selected
  to be saved from the tweet. As more functionality is added to the library, more information
  will be saved from each tweet.

  Properties:
  created_at -- datetime.datetime representing when the tweet was created/sent
  text -- string of the text of the tweet. This should be the full text, regardless of retweet/truncation
  screen_name -- string of the screen name of the tweeting user
  name -- string of the full name of the tweeting user
  is_retweet -- boolean whether or not this is a retweet
  rt_screen_name -- string of the screen name of the original author (if retweet)
  rt_name -- string of the full name of the original author (if retweet)
  hashtags -- list of any hashtags included in the text of the tweet (w/o the # symbol). Can be empty []
  """

  def __init__(self, tweet_dict):
    self._user = {}
    self._user["name"] = tweet_dict["user"]["name"]
    self._user["screen_name"] = tweet_dict["user"]["screen_name"]
    self._user["location"] = tweet_dict["user"]["location"]
    self._created_at = datetime.strptime(tweet_dict["created_at"], "%a %b %d %H:%M:%S %z %Y")
    self._is_retweet = False
    self._rt_user = None
    self._hashtags = []

    t = tweet_dict
    # check if this is a retweet, grab the original tweet
    # and user
    if "retweeted_status" in t:
      self._is_retweet = True
      t = t["retweeted_status"]
      self._rt_user = {
        "name": t["user"]["name"],
        "screen_name": t["user"]["screen_name"],
        "location": t["user"]["location"]
      }
    
    # is this a new 240 char tweet?
    if t["truncated"]:
      t = t["extended_tweet"]
      self._text = t["full_text"]
    else:
      self._text = t["text"]

    # get hashtags
    self._hashtags = [tag["text"] for tag in t["entities"]["hashtags"]]

    # get user-mentions
    self._user_mentions = []
    for u in t["entities"]["user_mentions"]:
      user = {
        "name": u["name"],
        "screen_name": u["screen_name"]
      }
      self._user_mentions.append(user)

  @property
  def created_at(self):
    """Get the timestamp of the tweet as a datetime.datetime."""
    return self._created_at

  @property
  def text(self):
    """Get the text of the tweet."""
    return self._text

  @property
  def screen_name(self):
    """Get the screen name of the tweet author."""
    return self._user["screen_name"] 

  @property
  def name(self):
    """Get the full name of the tweet author."""
    return self._user["name"]

  @property
  def is_retweet(self):
    return self._is_retweet
    
  @property
  def rt_screen_name(self):
    """Get the screen name of the retweeted user, or None if not a retweet."""
    if not self._is_retweet:
      return None
    return self._rt_user["screen_name"]

  @property
  def rt_name(self):
    """Get the full name of the retweeted user, or None if not a retweet."""
    if not self._is_retweet:
      return None
    return self._rt_user["name"]

  @property
  def hashtags(self):
    """Get a list of hastags in the tweet. Could be empty."""
    return self._hashtags 


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

  def __init__(self, queue, verbose=False, **options):
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
    """Parse incoming tweets from the streaming response and put them in a thread queue.

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
            elapsed = now - last_keep_alive
            last_keep_alive = now
            if verb: print("<< READER >> keep alive detected ({:.2f} sec elapsed); sleeping to yield.\n".format(elapsed))
            time.sleep(1)

    if verb: print("stopped running or connection closed") 
    self.running = False   

  def run(self):
    """Begin monitoring the Twitter stream.

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
          print(">> Connected to Twitter stream:", self.track)
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
        print(">> Exception occurred:", exc_info[1])
        break
    
    # cleanup
    self.running = False
    if resp: resp.close()
    self.new_session()
    self.queue.put(None)

    # Re-raise any exceptions not addressed
    if exc_info is not None:
      raise exc_info[0]
    
  def new_session(self):
    """Create a new session for the thread and assign the proper OAuth1 credentials. """
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
  on_data -- callback function used whenever a tweet is processed. Must accept a single parameter
    that is of type Tweet
  """
  
  def __init__(self, queue, on_data, verbose=True):
    super().__init__()
    self.queue = queue
    self.on_data = on_data
    self.verbose = verbose

  def run(self):
    """JSON decode tweet byte strings from the queue.

    Currently select fields are simply printed, but other data processing could be
    accomplished here instead.
    """
    while True:
      tweet_obj = self.queue.get()
      # if None is found, the producer thread has shut down
      if tweet_obj is None:
        self.queue.task_done()
        break
      try:
        tweet_dict = json.loads(tweet_obj)
      except json.JSONDecodeError as err:
        print("Found an object not properly JSON formatted...")
      else:
        self.on_data(Tweet(tweet_dict))
      finally:
        self.queue.task_done()
      

class TweetStrainer(object):
  """Connect to the Twitter stream and track the given terms.

  Wrapper class for the producer/consumer pair of classes _StreamFilterThread and 
  _TweetConsumerThread. 
  
  Instance members:
  track -- list of string terms to track
  on_data -- callback function passed to the consumer thread; called when a tweet
    is processed. Must accept a single parameter of type Tweet. If no callback is 
    provided, text of the tweet is printed by default.
  """
  
  def __init__(self, track, on_data=None):
    if isinstance(track, list):
      self.track = list(map(str, track))
    else:
      self.track = [str(track)]
    
    if on_data is None:
      self.on_data = lambda tweet: print(tweet.text)
    else:
      if not callable(on_data): raise TypeError("on_data parameter must be a callable")
      self.on_data = on_data

    self._queue = Queue()
    self._threads = [
      _StreamFilterThread(self._queue, track=self.track),
      _TweetConsumerThread(self._queue, self.on_data)
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
