from googleapiclient.discovery import build
import pandas as pd
import pymongo
import mysql.connector
import re
from datetime import timedelta
from datetime import datetime
import streamlit as st
from streamlit_option_menu import option_menu

st.set_page_config(layout='wide')
selected = option_menu(menu_title=None,
                       options=['Data Migration', 'Queries'],
                       orientation='horizontal'
                       )
try:
    if selected == 'Data Migration':
        st.title('Welcome to youtube channel data analytics')

        col1, col2 = st.columns([2, 1])
        with col1:
            channel_id = st.text_input("Enter the channel_id")

        with col2:
            st.write('''Few channel_ids''')
            st.write("UCChmJrVa8kDg05JfCmxpLRw")
            st.write("UCe4c5YQfFsCQVMsOnJOiTIw")
            st.write("UCKPCmSvWsXX13qfrnT0hoUQ")
            st.write("UCQiNyL7ik4FIlV2UCvojq0g")
            st.write("UCtoNXlIegvxkvf5Ji8S57Ag")


        # connecting to youtube api
        def connect_api():
            api_key = 'AIzaSyC_O6-x15MiT59OecnnQK2lCIXlbXf6M18'
            youtube = build('youtube', 'v3', developerKey=api_key)
            return youtube


        youtube = connect_api()


        def get_channel_stats(channel_id):

            request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=channel_id
            )
            responce = request.execute()

            data = dict(channel_name=responce['items'][0]['snippet']['title'],
                        channel_id=responce['items'][0]['id'],
                        subscription_count=responce['items'][0]['statistics']['subscriberCount'],
                        channel_views=responce['items'][0]['statistics']['viewCount'],
                        channel_description=responce['items'][0]['snippet']['description'],
                        Total_videos=responce['items'][0]['statistics']['videoCount'],
                        playlist_id=responce['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                        )

            return data


        with col1:
            name = get_channel_stats(channel_id)['channel_name']
            st.write("channel_id you passed : ", channel_id)
            st.write("Channel_name : ", name)


        def get_playlist_details(channel_id):
            all_data = []
            next_page_token = None
            next_page = True

            while next_page:
                request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()

                for i in response['items']:
                    data = dict(Playlist_id=i['id'],
                                Title=i['snippet']['title'],
                                channel_id=i['snippet']['channelId'],
                                channel_name=i['snippet']['channelTitle'],
                                published_at=i['snippet']['publishedAt'],
                                video_count=i['contentDetails']['itemCount']
                                )
                    all_data.append(data)
                next_page_token = response.get("nextPageToken")
                if next_page_token is None:
                    next_page = False
            return all_data


        def get_video_ids(channel_id):
            youtube = connect_api()
            playlist_id = get_channel_stats(channel_id)['playlist_id']
            video_ids = []
            next_page_token = None
            next_page = True
            while next_page:
                request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token)
                responce = request.execute()

                for i in responce['items']:
                    video_ids.append(i['contentDetails']['videoId'])
                next_page_token = responce.get('nextPageToken')
                if next_page_token is None:
                    next_page = False
            return video_ids


        def get_video_details(channel_id):
            youtube = connect_api()
            video_ids = get_video_ids(channel_id)
            video_stats = []
            for i in video_ids:
                request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=i
                )
                response = request.execute()

                data = dict(channel_id=response['items'][0]['snippet']['channelId'],
                            channel_name=response['items'][0]['snippet']['channelTitle'],
                            video_id=response['items'][0]['id'],
                            Video_Name=response['items'][0]['snippet']['title'],
                            Video_Description=response['items'][0]['snippet']['description'],
                            Tags=response['items'][0]['snippet'].get('tags'),
                            PublishedAt=response['items'][0]['snippet']['publishedAt'],
                            View_Count=response['items'][0]['statistics']['viewCount'],
                            Like_Count=response['items'][0]['statistics']['likeCount'],
                            Favorite_Count=response['items'][0]['statistics']['favoriteCount'],
                            Comment_Count=response['items'][0]['statistics'].get('commentCount'),
                            Duration=response['items'][0]['contentDetails']['duration'],
                            Thumbnail=response['items'][0]['snippet']['thumbnails']['standard']['url'],
                            Caption_Status=response['items'][0]['contentDetails'].get("caption", 'Not available'),
                            )
                video_stats.append(data)
            return video_stats


        def get_comments_details(video_ids):
            youtube = connect_api()
            comment_info = []

            try:
                for vid in video_ids:
                    request = youtube.commentThreads().list(
                        part="snippet",
                        videoId=vid,
                        maxResults=50)
                    response = request.execute()
                    for i in response['items']:
                        data = dict(comment_id=i['snippet']['topLevelComment']['id'],
                                    video_id=i['snippet']['videoId'],
                                    comment_text=i['snippet']['topLevelComment']['snippet']['textOriginal'],
                                    comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    comment_published=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                        comment_info.append(data)
            except:
                pass
            return comment_info


        myclient = pymongo.MongoClient(
            "mongodb+srv://satyanarayanajammu3:Satya4038@cluster0.klivjug.mongodb.net/?retryWrites=true&w=majority")
        db = myclient['youtube_project']


        def channel_details_to_mongodb(channel_id):
            chnl_details = get_channel_stats(channel_id)
            plst_details = get_playlist_details(channel_id)
            vid_ids = get_video_ids(channel_id)
            vid_details = get_video_details(channel_id)
            com_details = get_comments_details(vid_ids)

            col = db['channel_details']
            for i in col.find({}, {"_id": 0, 'channel_info': 1}):
                if i['channel_info']['channel_id'] == channel_id:
                    st.success('Data already stored')
                    break
            else:
                col.insert_one({"channel_info": chnl_details,
                                "playlist_details": plst_details,
                                "video_ids": vid_ids,
                                "video_details": vid_details,
                                "comment_details": com_details
                                })
                st.success("Data successfully stored in Mongodb")


        st.write("\n\n\n\n\n\n")
        st.write("Press the button to extract the data from youtube api and save it to Mongodb")

        if st.button("to Mongodb Atlas"):
            channel_details_to_mongodb(channel_id)

        config = {
            'host': 'localhost',
            'user': 'shyam4038',
            'password': 'shyam123'
        }
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.execute("create database if not exists youtube_project;")
        conn.close()


        def channel_table():
            config = {
                'host': 'localhost',
                'user': 'shyam4038',
                'password': 'shyam123',
                'database': 'YDB'
            }
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            cursor.execute("drop table if exists channel")
            conn.commit()
            create_query = '''create table if not exists channel(Channel_Name varchar(255),
                            Channel_Id varchar(255) primary key,
                                Subscription_Count bigint, 
                                Views bigint,
                                Total_Videos int,
                                Channel_Description text,
                                Playlist_Id varchar(50))'''
            cursor.execute(create_query)
            conn.commit()

            ch_list = []
            col = db['channel_details']
            for i in col.find({}, {"_id": 0, "channel_info": 1}):
                ch_list.append(i["channel_info"])
            df = pd.DataFrame(ch_list)

            for index, row in df.iterrows():
                insert_query = '''
                                insert into channel (Channel_Name,
                                Channel_Id ,
                                Subscription_Count, 
                                Views,
                                Total_Videos,
                                Channel_Description,
                                Playlist_Id
                                ) values (%s,%s,%s,%s,%s,%s,%s)'''
                values = (row['channel_name'],
                          row['channel_id'],
                          row['subscription_count'],
                          row['channel_views'],
                          row['Total_videos'],
                          row['channel_description'],
                          row['playlist_id'])
                cursor.execute(insert_query, values)
            conn.commit()


        def playlist_table():
            config = {
                'host': 'localhost',
                'user': 'shyam4038',
                'password': 'shyam123',
                'database': 'YDB'
            }
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            cursor.execute("drop table if exists playlist")
            conn.commit()
            create_query = '''create table if not exists playlist(playlist_id varchar(255) primary key,
                            channel_Id varchar(255) ,
                            playlist_name varchar(255),
                            channel_name varchar(255),
                            published_at varchar(255),
                            video_count varchar(255)
                            )'''
            cursor.execute(create_query)
            conn.commit()

            ch_list = []
            col = db['channel_details']
            for i in col.find({}, {"_id": 0, "playlist_details": 1}):
                for j in range(len(i['playlist_details'])):
                    ch_list.append(i["playlist_details"][j])
            df = pd.DataFrame(ch_list)

            for index, row in df.iterrows():
                insert_query = '''
                                insert into playlist (playlist_id,
                                channel_Id ,
                                playlist_name,
                                channel_name,
                                published_at,
                                video_count)
                                    values (%s,%s,%s,%s,%s,%s)'''
                values = (row['Playlist_id'],
                          row['channel_id'],
                          row['Title'],
                          row['channel_name'],
                          row['published_at'],
                          row['video_count'])
                cursor.execute(insert_query, values)
            conn.commit()


        def video_table():
            config = {
                'host': 'localhost',
                'user': 'shyam4038',
                'password': 'shyam123',
                'database': 'YDB'
            }
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            cursor.execute("drop table if exists video")
            conn.commit()
            create_query = '''create table if not exists video(
                                    video_id varchar(255) primary key,
                                    Video_Name varchar(255),
                                    channel_id varchar(255),
                                    channel_name varchar(255),
                                    Video_Description TEXT, 
                                    Published_Date DATETIME, 
                                    View_Count INT,
                                    Like_Count int,
                                    Favorite_Count int,
                                    Comment_Count int,
                                    Duration int,
                                    Thumbnail varchar(225), 
                                    Caption_Status varchar(255) 
                                    )'''
            cursor.execute(create_query)
            conn.commit()

            vd_list = []
            col = db['channel_details']
            for i in col.find({}, {"_id": 0, "video_details": 1}):
                for j in range(len(i['video_details'])):
                    vd_list.append(i["video_details"][j])
            df = pd.DataFrame(vd_list)

            def duration_to_seconds(duration):
                parts = re.findall(r'(\d+)([HMS])', duration)
                seconds = 0
                for value, unit in parts:
                    value = int(value)
                    if unit == 'H':
                        seconds += value * 3600
                    elif unit == 'M':
                        seconds += value * 60
                    elif unit == 'S':
                        seconds += value
                return seconds

            for index, row in df.iterrows():
                insert_query = '''
                        INSERT INTO video (
                                video_id ,
                                Video_Name ,
                                channel_id,
                                channel_name,
                                Video_Description  , 
                                Published_Date , 
                                View_Count ,
                                Like_Count ,
                                Favorite_Count ,
                                Comment_Count ,
                                Duration ,
                                Thumbnail , 
                                Caption_Status 
                                )
                        VALUES (%s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s)

                        '''
                published_at_datetime = datetime.fromisoformat(row['PublishedAt'].replace("Z", "+00:00"))

                mysql_datetime = published_at_datetime.strftime("%Y-%m-%d %H:%M:%S")
                values = (
                    row['video_id'],
                    row['Video_Name'],
                    row['channel_id'],
                    row['channel_name'],
                    row['Video_Description'],
                    mysql_datetime,
                    row['View_Count'],
                    row['Like_Count'],
                    row['Favorite_Count'],
                    row['Comment_Count'],
                    duration_to_seconds(row['Duration']),
                    row['Thumbnail'],
                    row['Caption_Status'])
                cursor.execute(insert_query, values)
            conn.commit()


        def comment_table():
            config = {
                'host': 'localhost',
                'user': 'shyam4038',
                'password': 'shyam123',
                'database': 'YDB'
            }
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            cursor.execute("drop table if exists comment")
            conn.commit()
            create_query = '''create table if not exists comment(
                                comment_id varchar(255) primary key,
                                video_id varchar(255),
                                comment_text TEXT,
                                comment_author varchar(255),
                                comment_published_date DATETIME
                                )'''
            cursor.execute(create_query)
            conn.commit()

            cmt_list = []
            col = db['channel_details']
            for i in col.find({}, {"_id": 0, "comment_details": 1}):
                for j in range(len(i['comment_details'])):
                    cmt_list.append(i["comment_details"][j])
            df = pd.DataFrame(cmt_list)

            for index, row in df.iterrows():
                insert_query = '''
                        INSERT INTO comment (
                            comment_id ,
                            video_id ,
                            comment_text ,
                            comment_author ,
                            comment_published_date 
                            )
                        VALUES (%s, %s, %s, %s, %s)   '''
                published_at_datetime = datetime.fromisoformat(row['comment_published'].replace("Z", "+00:00"))

                mysql_datetime = published_at_datetime.strftime("%Y-%m-%d %H:%M:%S")
                values = (
                    row['comment_id'],
                    row['video_id'],
                    row['comment_text'],
                    row['comment_author'],
                    mysql_datetime)
                cursor.execute(insert_query, values)
            conn.commit()


        def tables():
            channel_table()
            playlist_table()
            video_table()
            comment_table()


        st.write('\n\n\n\n\n\n')
        st.write("press the submit button to migrate the data to mysql Database")
        if st.button("to MySQL Database"):
            tables()
            st.success("Data successfully migrated to MySQL Database")


        def show_channel_table():
            ch_list = []
            col = db['channel_details']
            for i in col.find({}, {"_id": 0, "channel_info": 1}):
                ch_list.append(i["channel_info"])
            df = pd.DataFrame(ch_list)
            return st.dataframe(df)


        def show_playlist_table():
            ch_list = []
            col = db['channel_details']
            for i in col.find({}, {"_id": 0, "playlist_details": 1}):
                for j in range(len(i['playlist_details'])):
                    ch_list.append(i["playlist_details"][j])
            df = pd.DataFrame(ch_list)
            return st.dataframe(df)


        def show_video_table():
            vd_list = []
            col = db['channel_details']
            for i in col.find({}, {"_id": 0, "video_details": 1}):
                for j in range(len(i['video_details'])):
                    vd_list.append(i["video_details"][j])
            df = pd.DataFrame(vd_list)
            return st.dataframe(df)


        def show_comment_table():
            cmt_list = []
            col = db['channel_details']
            for i in col.find({}, {"_id": 0, "comment_details": 1}):
                for j in range(len(i['comment_details'])):
                    cmt_list.append(i["comment_details"][j])
            df = pd.DataFrame(cmt_list)
            return st.dataframe(df)


        st.write("\n\n\n\n\n\n")
        tbl = st.selectbox("Table", ['channel_table', 'playlist_table', 'video_table', 'comment_table'])
        if tbl == 'channel_table':
            show_channel_table()
        elif tbl == 'playlist_table':
            show_playlist_table()
        elif tbl == 'video_table':
            show_video_table()
        elif tbl == 'comment_table':
            show_comment_table()

    elif selected == 'Queries':
        st.title("Lets analyse the youtube channels data")
        st.write("\n\n\n\n\n\n")
        col1, col2 = st.columns([2, 1])
        with col2:
            qs = st.radio('Pick your question to get the analysis',
                          ['1.What are the names of all the videos and their corresponding channels?'
                              , '2. Which channels have the most number of videos, and how many videos do they have?'
                              , '3. What are the top 10 most viewed videos and their respective channels?'
                              ,
                           '4. How many comments were made on each video, and what are their corresponding video names?'
                              ,
                           '5. Which videos have the highest number of likes, and what are their corresponding channel names?'
                              ,
                           '6. What is the total number of likes for each video, and what are their corresponding video names?'
                              ,
                           '7. What is the total number of views for each channel, and what are their corresponding channel names?'
                              ,
                           '8. What are the names of all the channels that have published videos in the year 2022? Published_Date channel_name,avg(Duration)'
                              ,
                           '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                           '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
        with col1:
            config = {
                'host': 'localhost',
                'user': 'shyam4038',
                'password': 'shyam123',
                'database': 'YDB'
            }
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            if qs == '1.What are the names of all the videos and their corresponding channels?':
                select_query = ''' select Video_Name as Video_Name,channel_name as channel_name from video'''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['Video_Name', 'channel_name'])
                st.dataframe(df)

            elif qs == '2. Which channels have the most number of videos, and how many videos do they have?':
                select_query = ''' select channel_name , Total_Videos from channel order by Total_Videos desc limit 1'''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['channel_name', 'Total_Videos'])
                st.dataframe(df)

            elif qs == '3. What are the top 10 most viewed videos and their respective channels?':
                select_query = ''' select Video_Name , View_Count, channel_name from video order by View_Count desc limit 10'''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['Video_Name', 'View_Count', 'channel_name'])
                st.dataframe(df)

            elif qs == '4. How many comments were made on each video, and what are their corresponding video names?':
                select_query = ''' select Video_Name , Comment_Count from video '''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['Video_Name', 'Comment_Count'])
                st.dataframe(df)

            elif qs == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                select_query = ''' select Video_Name ,Like_Count, channel_name from video order by View_Count desc'''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['Video_Name', 'Like_Count', 'channel_name'])
                st.dataframe(df)

            elif qs == '6. What is the total number of likes for each video, and what are their corresponding video names?':
                select_query = ''' select Video_Name ,Like_Count from video '''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['Video_Name', 'Like_Count'])
                st.dataframe(df)

            elif qs == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                select_query = ''' select channel_name , Views from channel '''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['channel_name', 'Views'])
                st.dataframe(df)
            elif qs == '8. What are the names of all the channels that have published videos in the year 2022? Published_Date channel_name,avg(Duration)':
                select_query = ''' select distinct(channel_name) from video where year(Published_Date) = 2022'''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['channel_name'])
                st.dataframe(df)

            elif qs == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                select_query = ''' select channel_name,avg(Duration) from video group by channel_name'''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['channel_name', 'average_duration(s)'])
                st.dataframe(df)

            elif qs == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                select_query = ''' select Video_Name , Comment_Count,channel_name from video order by Comment_Count desc'''
                cursor.execute(select_query)
                a = cursor.fetchall()
                df = pd.DataFrame(a, columns=['Video_Name', 'Comment_Count', 'channel_name'])
                st.dataframe(df)
except KeyError:
    st.error("No channel exists with the channel_id")
    st.write("Try with another channel_id")

