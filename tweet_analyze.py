from config import Config
from tweet_strainer import TweetStrainer, Tweet

class TweetResponder(object):
  def __init__(self, hashtags):
    # input validation on hashtags: make it a list or a string
    self.hashtags = hashtags
    self.tweet_strainer = TweetStrainer(self.hashtags, self.process_tweet)
  
  def process_tweet(self, tweet):
    if tweet.is_retweet:
      print("RT:", tweet.screen_name, ">>", tweet.rt_screen_name, ">> ", end="")
    else:
      print(tweet.screen_name, ">> ", end="")
    print(tweet.text)
    print("#hashtags >>", ",".join(tweet.hashtags))
    print()
  
  def run(self):
    self.tweet_strainer.run()


if __name__ == "__main__":
  tr = TweetResponder("portland")
  tr.run()
