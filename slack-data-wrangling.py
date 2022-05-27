# import library
import os
import json
import glob
import numpy as np
import pandas as pd


#generate all channels in one dataframe
def parse_all_json(main_folder_path):
    df = pd.DataFrame()

    ## iterate through the group of folder
    for folder in os.listdir(main_folder_path):
        folder_path = os.path.join(main_folder_path, folder)

        if os.path.isdir(folder_path):
            ## iterate through each individual folder
            for file in os.listdir(folder_path):
                file = os.path.join(main_folder_path, folder, file)

                ## add channel columns name for each folder (channel)
                if file.endswith('.json'):
                    data = pd.read_json(file)
                    data['channel_name'] = folder
                    df = pd.concat([df,data])
                    
    return df

class data_cleaning:

    # data wrangling
    def datetime_wrangling(dataframe):
        ###this function is applied to summarise wrangling steps with datetime
        
        # convert ts to datetime from float
        dataframe['ts'] = pd.to_datetime(dataframe['ts'], unit='s').astype('datetime64[s]')
        
        # create a column for hour and minute of the day using the ts column
        dataframe['date']= dataframe['ts'].dt.strftime(r"%d/%m/%y")

        # create a column for hour and minute of the day using the ts column
        dataframe['time']= dataframe['ts'].dt.strftime('%H:%M:%S')
        
        # drop ts column
        dataframe.drop('ts', axis=1, inplace=True) 

        return dataframe
    
    # drop unnecessary columns
    def drop_columns(dataframe, drop_column):

        # drop columns not needed
        dataframe.drop(drop_column, axis=1, inplace=True)

        # filter out for the rows which has subtype values
        dataframe = dataframe[(dataframe.subtype != 'channel_join') & 
                                    (dataframe.subtype != 'channel_join') &
                                    (dataframe.subtype != 'channel_purpose') &
                                    (dataframe.subtype != 'thread_broadcast')]
        
        # drop subtype column with the values we don't need anymore
        dataframe.drop('subtype', axis=1, inplace=True) 
        
        return dataframe 

    # collecting attachment from attachment column
    def return_attachments(txt):
        """this function is applied to column attachments to extract links
        """
        try:
            dictionary = (txt)[0]
            if 'original_url' in dictionary:
                return dictionary.get('original_url', 'None')
        except:
            return 'None' 
    
    # get username
    def account_name(x):
        """this function is applied to column user_profile to extract real_name
        """
        if x != x:
            return 'noname'
        else:
            return x['name']

    # count reactions for each meassage
    def reactions_count(txt):
        """this function is applied to column reactions to count reactions
        """
        try:
            dictionary = eval(txt)[0]
            if 'reactions' in dictionary:
                return dictionary.get('reactions', 'None')
        except:
            return 'None'
    
    # get user reaction name
    def reactions_name(txt):
        """this function is applied to column reactions to count them
        """
        
        try:
            dictionary = eval(txt)[0]
            if 'name' in dictionary:
                return dictionary.get('name', 'None')
        except:
            return 'None'

    def clean_post_feature_eng(dataframe):
    
    # droppig unneccessary columns
        dataframe.drop(['reactions', 'reply_users', 'replies'], axis=1, inplace=True)
        
        # replace None values with zero
        dataframe['reply_count'] = dataframe['reply_count'].fillna(0)
        dataframe['reply_users_count'] = dataframe['reply_users_count'].fillna(0)
        dataframe['reply_count'] = dataframe['reply_count'].astype(int)
        dataframe['reply_users_count'] = dataframe['reply_users_count'].astype(int)
        
        # reordering columns
        dataframe = dataframe[['date', 'time', 'channel_name', 'user',  
                        'real_name', 'text', 'reply_count', 'reply_users_count',
                        'reactions_count', 'reactions_name', 'attachments'
                         ]]
        
        return dataframe




main_folder_path = r"C:\Users\Rija\SHIFTING\Slack project\raw_data"
data = parse_all_json(main_folder_path)

# export raw_data to csv file
data.to_csv(main_folder_path + r"\raw_data.csv", index= False)
print("export raw_data.csv completed!")

data = data_cleaning.datetime_wrangling(data)

drop_columns = ['type', 'client_msg_id', 'team', 'user_team',
             'source_team', 'blocks', 'upload', 'display_as_bot',
             'thread_ts', 'latest_reply', 'is_locked', 'subscribed',
             'parent_user_id', 'bot_id', 'bot_profile', 'last_read', 'edited',
             'purpose', 'inviter', 'topic', 'root', 'old_name', 'name', 'hidden',
             'files','x_files']

data = data_cleaning.drop_columns(data, drop_columns)
data['attachments'] = data['attachments'].apply(data_cleaning.return_attachments)
data['real_name'] = data['user_profile'].apply(data_cleaning.account_name)

# drop user_profile column
data.drop('user_profile', axis=1, inplace=True) 

data['reactions_count'] = data['reactions'].apply(data_cleaning.reactions_count)
data['reactions_name'] = data['reactions'].apply(data_cleaning.reactions_name)

# cleaning again after feature engineering
data = data_cleaning.clean_post_feature_eng(data)

# export data_clean to excel file
data.to_excel(main_folder_path + r"\clean_data.xlsx",sheet_name= "main" , index= False)
print("export clean_data.csv completed!")

