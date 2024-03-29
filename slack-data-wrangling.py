# import library
import os
import json
import numpy as np
import pandas as pd
import re 
from datetime import datetime
import pytz


#generate all channels in one dataframe
def parse_all_json(main_folder_path):
    dataframe = pd.DataFrame()

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
                    dataframe = pd.concat([dataframe,data])
                    
    return dataframe

class data_cleaning:

    # data wrangling
    def datetime_wrangling(dataframe):
        '''
        this function is applied to summarise wrangling steps with datetime
        '''
        new_ts= []
        # convert ts to datetime(localtime WIB) from float
        for i in dataframe['ts']:
            new = pd.to_datetime(i, unit='s', utc= True).astimezone(pytz.timezone('Asia/Jakarta'))
            new_ts.append(new)
        dataframe['new_ts'] = new_ts
        # create a column for hour and minute of the day using the ts column
        dataframe['date']= dataframe['new_ts'].dt.strftime(r"%d/%m/%y")

        # create a column for hour and minute of the day using the ts column
        dataframe['time']= dataframe['new_ts'].dt.strftime('%H:%M:%S')
        
        # create a column for the months of the year using the ts column
        dataframe['hour'] = dataframe['new_ts'].dt.hour

        # create a column for the day  using the ts column
        dataframe['day'] = dataframe['new_ts'].dt.strftime("%A")

        # drop ts column
        dataframe.drop(['ts','new_ts'], axis=1, inplace=True) 

        return dataframe
    
    # get username
    def real_name(x):
        """this function is applied to column user_profile to extract real_name
        """
        if x != x:
            return 'noname'
        else:
            return x['real_name']
    
    # define message is edited or not
    def is_edited(x):
        if pd.isna(x) != True:
            return 'edited'
        else:
            return np.nan
    
    # define message is reply or not
    def is_reply(x):
        if pd.isna(x) != True:
            return 'reply'
        else:
            return np.nan

# define function for task scorecard
def point_judge(message_text):
    TASK_POINT = 0
    split_text = message_text.split("\n")
    for i in split_text:
        if re.search(r"^[1-9]", i) or re.search(r"^•", i):
            # print(i)
            TASK_POINT += 1
            # print("point +1 for task")

            #checking for OKR
            if re.search(r"\(.*\)", i) != None:
                TASK_POINT += 1
                # print("point +1 for OKR")
            else: 
                None
            
            # checking for progress checklist
            if re.search(r"\:.*\:", i) != None:
                TASK_POINT += 1
                # print("point +1 for progress")
            else:
                None
        else:
            None
    return TASK_POINT

# define function for task scorecard
def time_point(time):
    TIME_POINT = 0
    if time >= 9:
        TIME_POINT -= 1
    else:
        TIME_POINT += 1
    return TIME_POINT 

def count_absent_day(dataframe):
    pai_member = list(dataframe['real_name'].unique())
    
    # weekdays = dataframe['date'].unique()
    weekdays = dataframe[(dataframe['day'] != 'Saturday') & (dataframe['day'] != 'Sunday')]['date'].unique()
    member_each_day = []
    for day in weekdays:
        per_day = dataframe[dataframe['date'] == day]
        member_name_list= list(per_day['real_name'].unique())
        # print(len(member_name_list))
        member_each_day.append(member_name_list)

    member_team = []
    total_absen = []

    for name in pai_member:
        ABSEN_DAY = 0
        for i in range(0,len(member_each_day)):
            if name not in member_each_day[i]:
                ABSEN_DAY += 1
                        
            else: 
                None
        
        member_team.append(name)
        total_absen.append(ABSEN_DAY)

    absen_df = pd.DataFrame(
        {'name' : member_team,
        'weekdays_absen' : total_absen } 
    ).sort_values('name')
    
    return absen_df


# set folder path
main_folder_path = r"C:\Users\Fujitsu\Project\slack_DA\stayconnected_nov"
# join all files to one dataframe
data = parse_all_json(main_folder_path)

# export raw_data to csv file
# data.to_csv(main_folder_path + r"\raw_data_PAI.csv", index= False)
# print("export raw_data.csv completed!")

use_cols = ['ts','user_profile','type','text','edited','channel_name','parent_user_id']
data = data[use_cols]

data = data_cleaning.datetime_wrangling(data)

data['real_name'] = data['user_profile'].apply(data_cleaning.real_name)
data['is_edited'] = data['edited'].apply(data_cleaning.is_edited)
data['is_reply'] = data['parent_user_id'].apply(data_cleaning.is_reply)
data['task_point'] = data['text'].apply(point_judge)
data['time_point'] = data['hour'].apply(time_point)

# replace time point for reply message 
data.loc[data['is_reply'] == 'reply', 'time_point'] = 0

# # replace time point for edited message 
# data.loc[data['is_edited'] == 'edited', 'time_point'] = 0

# drop unnecassary column
data.drop(['parent_user_id','user_profile','edited'], axis= 1, inplace= True)

# reordering column
data = data[['date','time','day','hour','real_name','type','text','is_edited','is_reply','task_point','time_point']]

# create dataframe for absen day
absen_df = count_absent_day(data)

# export data_clean to excel file
writer = pd.ExcelWriter(main_folder_path + r"\clean_data_nov.xlsx", engine='xlsxwriter')
data.to_excel(writer, sheet_name= "recap", index= False)
absen_df.to_excel(writer, sheet_name= "absent_day", index= False)
writer.save()
print("file export completed!")