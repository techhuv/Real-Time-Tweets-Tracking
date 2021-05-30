import tweepy,json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import csv
import datetime
import json
import os
import pandas as pd
from datetime import timezone
import itertools
import time

######################
# Enter your keys here
#Consumer Secret Key
csecret=""
#Consumer Key
ckey=""
#Access Token
atoken=""
#Access Token Secret
asecret=""

##########################
# Enter Your keywords here
keywords = ["python"]
req_interval = 300 # 300 seconds = 5 Min
##########################


summary_columns=['UTC Timestamp','Time Start','Interval Seconds','Keyword','Total Number of Accounts / Authors',
                                   'Total Number of Tweets','Max # of Followers','Total Number of Followers',
                                   'Total Number of Impressions','Elon Musk Flag']

start_time = datetime.datetime.now()
summary_data = []

class listener(StreamListener):

    def on_data(self, data):
        
        global start_time
        global summary_data
        global req_interval
        
        all_data = json.loads(data)

        Elon_Musk_Flag = 0
        tweet = all_data["text"]
        
        username = all_data["user"]["screen_name"]
        
        if(username == 'elonmusk'):
            Elon_Musk_Flag = 1
    
        date = all_data["created_at"]
    
        tt = datetime.datetime.strptime(date,'%a %b %d %X %z %Y')
        timestamp = int(tt.timestamp())
        
        date_detailed = datetime.datetime.strptime(date,'%a %b %d %X %z %Y').strftime('%d/%m/%Y %I:%M:%S %p')

        followers = all_data["user"]["followers_count"]
        
        if all_data["user"]["verified"]:
            verified = 'Yes'
        else:
            verified = 'No'

        retweet = all_data["retweet_count"]
        name = all_data["user"]["name"]
        tweet_url = 'https://twitter.com/'+all_data["user"]["screen_name"]+'/status/'+str(all_data["id"])
        
        fields_sheet_1 = ['Time Stamp','Date','Tweet','Followers','Elon_Musk_Flag']
        
        # data rows of csv file
        rows_sheet_1 = [[timestamp, date_detailed, tweet, followers, Elon_Musk_Flag]]
        rows_sheet_2 = [[date, tweet, username, followers]]
        summary_data.append(rows_sheet_2)
        
        # name of csv file
        filename = 'Detailed_'+datetime.datetime.now().strftime("%Y-%m-%d")+".csv"
    
        print("Current Time =", datetime.datetime.now().strftime("%H:%M:%S"))
        current_time = datetime.datetime.now()
        
        difference = current_time - start_time
        
        if difference.total_seconds() > req_interval:
            print('----------- 5 Minute Elapsed -----------')
            start_time = current_time
            self.write_summary_feed(summary_data,difference.total_seconds())
            
        # writing to csv file
        with open(filename, 'a+',encoding="utf-8") as csvfile:
            # creating a csv writer object
            csvwriter = csv.writer(csvfile)
            print('Data Written')
            
            # writing the fields
            if not(os.path.exists(filename) and os.path.getsize(filename) > 0):
                csvwriter.writerow(fields_sheet_1)
            
            # writing the data rows
            csvwriter.writerows(rows_sheet_1)

        return True
    
    def write_summary_feed(self,data,interval):

        
        filename = 'Summary_'+datetime.datetime.now().strftime("%Y-%m-%d")+".csv"
        
        merged = list(itertools.chain.from_iterable(summary_data))
        temp_cols = ['Date','Tweet','Username','Followers']
        temp = pd.DataFrame(merged,columns=temp_cols)
        
        temp['Date'] = pd.to_datetime(temp['Date'], format='%a %b %d %X %z %Y')
        # UTC Timestamp
        timestamp = int(temp.Date[0].timestamp())
        
        temp['Date'] = temp['Date'].dt.strftime('%I:%M:%S %p')
        
        # Time Start
        time_start = temp.Date[0]
        
        # Interval Seconds
        interval_seconds = int(interval)

        # Total Number of Accounts / Authors
        total_no_accounts = len(temp.Username.unique())

        # Total Number of Tweets
        total_no_tweets = len(temp)

        # Max number of Followers
        max_followers = temp.Followers.max()

        # Total Number of Followers
        unique_df = temp.drop_duplicates(subset=['Username'], keep='first')
        total_no_followers = unique_df.Followers.sum()

        # Total Number of Impressions
        frequency_df = temp.groupby(['Username','Followers']).count()
        frequency_df.reset_index(inplace=True)
        frequency_df['Reach'] = frequency_df['Followers'] * frequency_df['Tweet']
        total_impression = frequency_df['Reach'].sum()

        # Elon Musk Flag
        elon_musk_flag = 1 if 'elonmusk' in temp.Username.tolist() else 0
        
        # Make a dataframe
        global summary_columns
        global keywords
        
        df_summary = pd.DataFrame([[timestamp,time_start,interval_seconds,keywords,total_no_accounts,total_no_tweets,max_followers,total_no_followers,total_impression,elon_musk_flag]],columns=summary_columns)
        
    
        # writing the fields
        if not(os.path.exists(filename) and os.path.getsize(filename) > 0):
            df_summary.to_csv(filename, mode = 'a',index=False)
        else:
            df_summary.to_csv(filename, mode = 'a', header = False, index=False)

        print(' ------- Summary Data Written -------')
            
    def on_error(self, status):
        print (status)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

while(True):
    try:
        twitterStream = Stream(auth, listener())
        twitterStream.filter(track=keywords)
    except:
        print('Exception Occured! Restarted Fetching')
