import tweepy
import time
import json
import pandas as pd


class GetUserTweets:
    def __init__(self, bearer_token, consumer_key, consumer_secret, access_token, access_token_secret):
        self.bearer_token = bearer_token
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        self.client = tweepy.Client(bearer_token=self.bearer_token,
                                    consumer_key=self.consumer_key,
                                    consumer_secret=self.consumer_secret,
                                    access_token=self.access_token,
                                    access_token_secret=self.access_token_secret)
        self.user_id = []
        self.tweets = []
        self.tweets_in_json = []
        self.tweets_in_dataframe = pd.DataFrame()

    # get user id
    def get_user_id(self, user_name):
        """
        according to user name to get user id that is needed for getting tweets
        :param user_name: a list of user name
        :return: user id
        """

        for username in user_name:
            get_user = self.client.get_user(username=username)
            self.user_id.append(get_user.data.id)

        print(self.user_id)
        return self.user_id

    def get_users_tweets(self, user_id, max_results=5, until_id=None):
        """
        get tweets from a user with the oldest tweet_id
        :param user_id: a list of user ids
        :param max_results: default 5 tweets
        :param until_id: the oldest tweet id. default is None.
        :return: tweets in dict structure
        """

        request_time = 0  # the number of requests
        i = 0

        while (request_time <= 100) & (i != 99):  # the maximum number of requests that you want to send

            if until_id is None:  # send request to get the latest tweets
                for i in range(0, 100):  # try to send maximum 100 request
                    try:
                        tweet = self.client.get_users_tweets(id=user_id,
                                                             max_results=max_results,
                                                             expansions='author_id',
                                                             tweet_fields='created_at')
                    except tweepy.RateLimitError:
                        print("...sleeping for 15min due to Twitter API rate limit")
                        time.sleep((15 * 60) + 1)
                        continue

                    if tweet.data is not None:  # successfully get response
                        self.tweets.append(tweet)
                        until_id = self.tweets[-1].meta['oldest_id']  # update until_id as the oldest one

                        print('API responses at the %sth request' % (i + 1))
                        request_time = request_time + (i + 1)

                        break

                    elif i == 99:  # if send 99 requests and still get no response, then break the loop
                        print('You have gotten the oldest tweet.')
                        break

            else:  # send request to get tweets older than the until_id
                if len(self.tweets) == 0:
                    for i in range(0, 100):
                        try:
                            tweet = self.client.get_users_tweets(id=user_id,
                                                                 max_results=max_results,
                                                                 until_id=until_id,
                                                                 expansions='author_id',
                                                                 tweet_fields='created_at')
                        except tweepy.RateLimitError:
                            print("...sleeping for 15min due to Twitter API rate limit")
                            time.sleep((15 * 60) + 1)
                            continue

                        if tweet.data is not None:
                            self.tweets.append(tweet)

                            print('API responses at the %sth request' % (i + 1))
                            request_time = request_time + (i + 1)

                            break

                        elif i == 99:
                            print('You have gotten the oldest tweet.')
                            break
                else:
                    until_id = self.tweets[-1].meta['oldest_id']
                    for i in range(0, 100):
                        try:
                            tweet = self.client.get_users_tweets(id=user_id,
                                                                 max_results=max_results,
                                                                 until_id=until_id,
                                                                 expansions='author_id',
                                                                 tweet_fields='created_at')
                        except tweepy.RateLimitError:
                            print("...sleeping for 15min due to Twitter API rate limit")
                            time.sleep((15 * 60) + 1)
                            continue

                        if tweet.data is not None:
                            self.tweets.append(tweet)

                            print('API responses at the %sth request' % (i + 1))
                            request_time = request_time + (i + 1)

                            break

                        elif i == 99:
                            print('You have gotten the oldest tweet.')
                            break

        return self.tweets

    def get_users_latest_tweets(self, user_id, since_id, max_results=5):
        """
        get the newest tweets from a user with the newest tweet_id
        :param user_id: a user id
        :param max_results: default 5 tweets
        :param since_id: returns results with a Tweet ID greater than the specified ‘since’ Tweet ID
        :return: tweets in dict structure
        """

        request_time = 0  # the number of requests
        self.tweets = []
        i = 0

        while (request_time <= 100) & (i != 99):  # the maximum number of requests that you want to send

            if len(self.tweets) == 0:
                for i in range(0, 100):
                    try:
                        tweet = self.client.get_users_tweets(id=user_id,
                                                             max_results=max_results,
                                                             since_id=since_id,
                                                             expansions='author_id',
                                                             tweet_fields='created_at')
                    except tweepy.RateLimitError:
                        print("...sleeping for 15min due to Twitter API rate limit")
                        time.sleep((15 * 60) + 1)
                        continue

                    if tweet.data is not None:
                        self.tweets.append(tweet)

                        print('API responses at the %sth request' % (i + 1))
                        request_time = request_time + (i + 1)

                        break

                    elif i == 99:
                        print('You have gotten the newest tweet.')
                        break
            else:
                until_id = self.tweets[-1].meta['oldest_id']
                for i in range(0, 100):
                    try:
                        tweet = self.client.get_users_tweets(id=user_id,
                                                             max_results=max_results,
                                                             since_id=since_id,
                                                             until_id=until_id,
                                                             expansions='author_id',
                                                             tweet_fields='created_at')
                    except tweepy.RateLimitError:
                        print("...sleeping for 15min due to Twitter API rate limit")
                        time.sleep((15 * 60) + 1)
                        continue

                    if tweet.data is not None:
                        self.tweets.append(tweet)

                        print('API responses at the %sth request' % (i + 1))
                        request_time = request_time + (i + 1)

                        break

                    elif i == 99:
                        print('You have gotten the newest tweet.')
                        break

        return self.tweets

    def store_tweets_in_json(self):
        """
        convert tweets obtained from get_users_tweets() to json
        :return: tweets in dict format and store json file to local
        """

        self.tweets_in_json = []

        for every_tweet in self.tweets:
            for tweet in every_tweet.data:
                obj = {}
                obj['author_id'] = tweet.author_id
                obj['tweet_id'] = tweet.id
                obj['text'] = tweet.text
                obj['created_at'] = tweet.created_at.isoformat().replace('+00:00', 'Z')
                self.tweets_in_json.append(obj)

        with open('../data_tweets/MyTweets.json', 'a') as f:
            json.dump(self.tweets_in_json, f, indent=4)

        return self.tweets_in_json

    def store_tweets_in_dataframe(self):
        """
        convert tweets in dataframe and store csv data to local file
        :return: tweets in data frame format and store csv file to local
        """

        if len(self.tweets_in_json) != 0:
            self.tweets_in_dataframe = pd.DataFrame(self.tweets_in_json)
            self.tweets_in_dataframe.to_csv('../data_tweets/MyTweets.csv')
            return self.tweets_in_dataframe

        else:
            print("You need to call store_tweets_in_json() method first.")


# the below doesn't run when script is called via 'import'
if __name__ == '__main__':
    import __path
    from twitter_credentials import bearer_token, consumer_key, consumer_secret, access_token, access_token_secret

    get_tweets = GetUserTweets(bearer_token, consumer_key, consumer_secret, access_token, access_token_secret)

    # # get user id test
    # user_name = ['elonmusk', 'WSJmarkets']
    # get_tweets.get_user_id(user_name)

    # get latest tweets test
    user_id = '44196397'  # elon musk
    get_tweets.get_users_latest_tweets(user_id, since_id='1520645386427195392', max_results=100)
    print(get_tweets.tweets)

    # store tweets in json test
    get_tweets.store_tweets_in_json()
    print(get_tweets.tweets_in_json)

    # store tweets in csv test
    get_tweets.store_tweets_in_dataframe()
    print(get_tweets.tweets_in_dataframe)
