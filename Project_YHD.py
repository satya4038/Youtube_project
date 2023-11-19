import streamlit as st
from googleapiclient.discovery import build
import pymongo
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd

# 1. Streamlit Application Title
st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")

# 2. Enter the Channel ID
channel_id = st.text_input("Enter YouTube Channel ID")

# Placeholder to store retrieved data
channel_data = {}
videos_data = []

# Placeholder for channel_response
channel_response = None

# 3. Retrieve the Data
if st.button("Retrieve Data"):
    try:
        # Retrieve Channel Data
        youtube = build('youtube', 'v3', developerKey='AIzaSyC_O6-x15MiT59OecnnQK2lCIXlbXf6M18')  # Replace with your API key
        channel_request = youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=channel_id
        )
        channel_response = channel_request.execute()
        channel_data = channel_response['items'][0]['snippet']
        st.subheader("Channel Details:")
        st.write("Channel Name:", channel_data.get('title', ''))
        st.write("Channel ID:", channel_id)
        st.write("Subscription Count:", channel_response['items'][0]['statistics'].get('subscriberCount', 0))
        st.write("Channel Views:", channel_response['items'][0]['statistics'].get('viewCount', 0))
        st.write("Channel Description:", channel_data.get('description', ''))
        st.write("Playlist ID:", channel_response['items'][0]['contentDetails'].get('relatedPlaylists', {}).get('uploads', ''))

        # Retrieve Video Data
        videos_request = youtube.search().list(
            part='id',
            channelId=channel_id.strip(),
            maxResults=10
        )
        videos_response = videos_request.execute()

        video_ids = [item['id']['videoId'] for item in videos_response['items']]

        for video_id in video_ids:
            video_request = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=video_id
            )
            video_response = video_request.execute()
            video_data = video_response['items'][0]['snippet']
            videos_data.append({
                'Video_ID': video_id,
                'Video_Name': video_data.get('title', ''),
                'Video_Description': video_data.get('description', ''),
                'Tags': video_data.get('tags', []),
                'PublishedAt': video_data.get('publishedAt', ''),
                'View_Count': video_response['items'][0]['statistics'].get('viewCount', 0),
                'Like_Count': video_response['items'][0]['statistics'].get('likeCount', 0),
                'Favorite_Count': video_response['items'][0]['statistics'].get('favoriteCount', 0),
                'Comment_Count': video_response['items'][0]['statistics'].get('commentCount', 0),
                'Duration': video_response['items'][0]['contentDetails'].get('duration', ''),
                'Caption_Status': video_data.get('localized', {}).get('localized', 'Not Available'),
                'Comments': []  # To be filled later
            })

        st.subheader("Video Details:")
        for video_data in videos_data:
            st.write(f"Video ID: {video_data['Video_ID']}")
            st.write(f"Video Name: {video_data['Video_Name']}")
            st.write(f"Video Description: {video_data['Video_Description']}")
            st.write(f"Published At: {video_data['PublishedAt']}")
            st.write(f"View Count: {video_data['View_Count']}")
            st.write(f"Like Count: {video_data['Like_Count']}")
            st.write(f"Favorite Count: {video_data['Favorite_Count']}")
            st.write(f"Comment Count: {video_data['Comment_Count']}")
            st.write(f"Duration: {video_data['Duration']}")
            st.write(f"Caption Status: {video_data['Caption_Status']}")
            st.write("-----")

            # 4. Retrieve 100 Comments for Each Video
            comments_request = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_data['Video_ID'],
                maxResults=100
            )
            comments_response = comments_request.execute()
            if 'items' in comments_response:
                for comment in comments_response['items']:
                    comment_data = comment['snippet']['topLevelComment']['snippet']
                    video_data['Comments'].append({
                        'Comment_ID': comment_data.get('commentId', ''),
                        'Comment_Text': comment_data.get('textDisplay', ''),
                        'Comment_Author': comment_data.get('authorDisplayName', ''),
                        'Comment_PublishedAt': comment_data.get('publishedAt', '')
                    })

        # 5. Display Comments Details
        st.subheader("Comments Details:")
        for video_data in videos_data:
            st.write(f"Video ID: {video_data['Video_ID']}")
            for comment_data in video_data['Comments']:
                st.write(f"Comment ID: {comment_data['Comment_ID']}")
                st.write(f"Comment Text: {comment_data['Comment_Text']}")
                st.write(f"Comment Author: {comment_data['Comment_Author']}")
                st.write(f"Comment Published At: {comment_data['Comment_PublishedAt']}")
                st.write("-----")

        # 6. Display the "Store Data in MongoDB" button
        if st.button("Store Data in MongoDB"):
            try:
                with st.spinner("Storing data in MongoDB..."):
                    # Replace the connection string with your MongoDB local connection string
                    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
                    db = mongo_client['storagelake']
                    collection = db['channel_data']
                    collection.insert_one({
                        'Channel_ID': channel_id,
                        'Channel_Details': channel_data,
                        'Videos': videos_data
                    })

                st.success("Data stored successfully in MongoDB!")
            except Exception as e:
                st.error(f"Error storing data in MongoDB: {str(e)}")

    except Exception as e:
        st.error(f"Error retrieving channel data: {str(e)}")

# 7. Migrate Data from MongoDB to MySQL
migrate_data_mysql = st.button("Migrate Data to MySQL")
if migrate_data_mysql:
    try:
        if not channel_response:
            st.warning("Please retrieve data first before migrating to MySQL.")

        # Allow the user to select a channel for migration
        selected_channel_id = st.selectbox("Select a Channel ID for Migration", [channel_id])

        # Local MongoDB connection
        mongo_client_local = pymongo.MongoClient("mongodb://localhost:27017/")
        db_local = mongo_client_local['storagelake']
        collection_local = db_local['channel_data']

        # Fetch data from local MongoDB
        data = collection_local.find_one({'Channel_ID': selected_channel_id})
        channel_data = data['Channel_Details']
        videos_data = data['Videos']

        # MySQL connection details
        sql_host = 'localhost'
        sql_user = 'shyam4038'
        sql_password = 'shyam123'
        sql_database = 'storagelake'

        # Establish connection to MySQL
        cnx = mysql.connector.connect(
            host=sql_host,
            user=sql_user,
            password=sql_password,
            database=sql_database
        )

        cursor = cnx.cursor()

        # Create MySQL Tables (if not exists)
        create_channel_table_query = """
            CREATE TABLE IF NOT EXISTS channels (
                channel_id VARCHAR(255) PRIMARY KEY,
                channel_name VARCHAR(255),
                subscription_count INT,
                channel_views INT,
                channel_description TEXT,
                playlist_id VARCHAR(255)
            )
        """
        cursor.execute(create_channel_table_query)

        create_videos_table_query = """
            CREATE TABLE IF NOT EXISTS videos (
                video_id VARCHAR(255) PRIMARY KEY,
                channel_id VARCHAR(255),
                video_name VARCHAR(255),
                video_description TEXT,
                tags TEXT,
                published_at DATETIME,
                view_count INT,
                like_count INT,
                favorite_count INT,
                comment_count INT,
                duration VARCHAR(255),
                caption_status VARCHAR(255),
                FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
            )
        """
        cursor.execute(create_videos_table_query)

        create_comments_table_query = """
            CREATE TABLE IF NOT EXISTS comments (
                comment_id VARCHAR(255) PRIMARY KEY,
                video_id VARCHAR(255),
                comment_text TEXT,
                comment_author VARCHAR(255),
                comment_published_at DATETIME,
                FOREIGN KEY (video_id) REFERENCES videos(video_id)
            )
        """
        cursor.execute(create_comments_table_query)

        # Migrate Data to MySQL
        for video_data in videos_data:
            # Insert data into 'channels' table
            cursor.execute("""
                INSERT INTO channels (channel_id, channel_name, subscription_count, channel_views, 
                                     channel_description, playlist_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (selected_channel_id, channel_data.get('title', ''),
                  channel_response['items'][0]['statistics'].get('subscriberCount', 0),
                  channel_response['items'][0]['statistics'].get('viewCount', 0),
                  channel_data.get('description', ''),
                  channel_response['items'][0]['contentDetails'].get('relatedPlaylists', {}).get('uploads', '')))

            # Insert data into 'videos' table
            cursor.execute("""
                INSERT INTO videos (video_id, channel_id, video_name, video_description, tags,
                                    published_at, view_count, like_count, favorite_count,
                                    comment_count, duration, caption_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (video_data['Video_ID'], selected_channel_id, video_data['Video_Name'],
                  video_data['Video_Description'],
                  ','.join(video_data['Tags']), video_data['PublishedAt'], video_data['View_Count'],
                  video_data['Like_Count'], video_data['Favorite_Count'], video_data['Comment_Count'],
                  video_data['Duration'], video_data['Caption_Status']))

            # Insert data into 'comments' table
            for comment_data in video_data['Comments']:
                cursor.execute("""
                    INSERT INTO comments (comment_id, video_id, comment_text, comment_author,
                                         comment_published_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (comment_data['Comment_ID'], video_data['Video_ID'], comment_data['Comment_Text'],
                      comment_data['Comment_Author'], comment_data['Comment_PublishedAt']))

        # Commit changes and close the MySQL connection
        cnx.commit()
        cnx.close()

        st.success("Data migrated and stored successfully in MySQL!")

    except Exception as e:
        st.error(f"Error migrating data to MySQL: {str(e)}")

# 8. Enter SQL Queries and Display Results
sql_query = st.text_input("Enter SQL Query")
execute_query = st.button("Execute Query")
if execute_query:
    try:
        # Connect to MySQL
        sql_host = 'localhost'
        sql_user = 'shyam4038'
        sql_password = 'shyam123'
        sql_database = 'storagelake'

        engine = create_engine(f'mysql+pymysql://{sql_user}:{sql_password}@{sql_host}/{sql_database}')

        # Execute SQL Query and Display Results
        df = pd.read_sql_query(sql_query, engine)
        st.subheader("Query Results:")
        st.write(df)

    except Exception as e:
        st.error(f"Error executing SQL query: {str(e)}")
