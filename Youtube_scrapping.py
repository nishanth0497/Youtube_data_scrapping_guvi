import pymongo
import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
#import pyodbc    ------If you are using MQSSN
from googleapiclient.errors import HttpError
import pandas as pd
import mysql.connector as sql
from datetime import datetime
import plotly.express as px


api_key = "AIzaSyAiOn6srYcrU02LZ3rn4N25Gwr9aqxvbIg"
youtube = build('youtube','v3',developerKey=api_key)

# CREATING A SQL CONNECTION WITH SPECIFIC DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="nishanth0011",
                   auth_plugin='caching_sha2_password',
                   port=3306                   
                  )

mycursor = mydb.cursor(buffered=True)

database_query ="""CREATE DATABASE IF NOT EXISTS youtube_db"""
mycursor.execute(database_query)
mycursor.execute("USE youtube_db")
mydb.commit()

#CREATING A MONGODB SERVER CONNECTION WITH A SPECIFIC DATABASE
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['youtube_data']

#FUNCTIONS TO RETRIEVE YOUTUBE DETAILS:

def get_channel_details(chanel_id):
    channel_data = []
    temp_ch_details = {} 
    response =  youtube.channels().list(id=chanel_id, part='snippet, statistics,contentDetails').execute()
    
    for i in range(len(response['items'])):
        temp_ch_details = dict(Channel_id = chanel_id,
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        channel_data.append(temp_ch_details)
    
    return channel_data



def get_channel_videos(chanel_id):
  video_ids = []
  channel_detail = youtube.channels().list(id=chanel_id, part='contentDetails').execute()
  playlist_id = channel_detail['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token = None
  while True:
    videos_list = youtube.playlistItems().list(playlistId=playlist_id, 
                                            part='snippet', 
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
    
    for i in range(len(videos_list['items'])):
        video_ids.append(videos_list['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token = videos_list.get('nextPageToken')

    if next_page_token == None:
      break
    
  return video_ids


def get_video_details(v_ids):
    video_info = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_info.append(video_details)
    return video_info


def get_comments_details(video_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=video_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for i in response['items']:
                data = dict(Comment_id = i['id'],
                            Video_id = i['snippet']['videoId'],
                            Comment_text = i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = i['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = i['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = i['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


# STREAMLIT PAGE CONTENTS

st.set_page_config(page_title= "Data harvesting from Youtube",
                   layout= "centered",
                   initial_sidebar_state= "auto",
                   menu_items={'About': """By Nishanth"""})


with st.sidebar:
    selected = option_menu("Menu", ["Home","Extract", "Transform","Analysis"], 
                           icons=["house-door-fill","tools","card-text"],
                           #default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px", "--hover-color": "#0E709A",  "font-family": "'EB Garamond', serif",},
                                   "icon": {"font-size": "8px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#c84a01"}})


# STREAMLIT HOME PAGE:
if selected == "Home":

    st.header(":blue[_Welcome_]", divider='gray')
    st.markdown("#### :orange[ This site is designed to retrive data from youtube and analyse it.]")
    st.markdown("#### :orange[It uses youtube API, with which data of a channel is extracted. It is then stored in MongoDB. The data is migrated to SQL, from where analysis is done for different scenarios.]")
    st.markdown("#### :orange[The results are displayed using streamlit. You can use the sidebar to navigate through the menu.]")

# STREAMLIT EXTRACT PAGE:
if selected == "Extract":
    
        st.markdown("#### :orange[ Enter a :blue[_channel id_] of your choice]")
        st.header("",divider='grey')
        channel_id = st.text_input("Go to your fav channel and grab the id. Let's MATRIX it", placeholder="channel id goes here").split(',')

        if channel_id and st.button("Extract Data"):
            channel_details = get_channel_details(channel_id)
            st.write(f'#### Here is your fav channel :blue["{channel_details[0]['Channel_name']}"]')
            st.snow()
            st.dataframe(channel_details, hide_index=True)

        st.markdown("##### :orange[Once Extracted, click below to upload the data to :blue[_MongoDB_]]")

        if st.button("Upload to MongoDB"):
            with st.spinner('Uploading...'):
                channel_details = get_channel_details(channel_id)
                video_ids = get_channel_videos(channel_id)
                video_details = get_video_details(video_ids)
                
                def comments():
                    comment_detail = []
                    for i in video_ids:
                        comment_detail+= get_comments_details(i)
                    return comment_detail
                
                comm_details = comments()

                db.channel_details.insert_many(channel_details)

                db.video_details.insert_many(video_details)

                db.comments_details.insert_many(comm_details)

                st.success("Upload successful !!")
                st.balloons()
      

#TRANFORM PAGE DETAILS
if selected == "Transform":     
        st.markdown("#### :orange[Choose a Channel Name to upload to :blue[_SQL_]]")
        
        channel_names = channel_names()
        user_input = st.selectbox("Choose a channel from the list",options= channel_names)
        
        def insert_into_channels():
                channel_query = """CREATE TABLE IF NOT EXISTS channels(Channel_id VARCHAR(255),
                                                              Channel_name VARCHAR(255),
                                                              Playlist_id TEXT,
                                                              Subscribers INT,
                                                              Views INT,
                                                              Total_videos INT,
                                                              Description TEXT,
                                                              Country VARCHAR(255))"""
                mycursor.execute(channel_query)
                mydb.commit()
                collections = db.channel_details
                query = """INSERT INTO channels (Channel_id , Channel_name ,  Playlist_id , Subscribers , Views , Total_videos , Description , Country  )VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                for i in collections.find({"Channel_name" : user_input,},{"Channel_id":1, "Channel_name":1,  "Playlist_id":1, "Subscribers":1, "Views":1, "Total_videos":1, "Description":1, "Country":1, "_id":0}):
                    mycursor.execute(query,tuple(i.values()))
                    mydb.commit()
                
        def insert_into_videos():
            collections = db.video_details
            query1 = """INSERT INTO videos(Channel_name , Channel_id, Video_id, Title, Thumbnail, Description, Published_date, Views, Likes, Comments, Favorite_count, Definition, Caption_status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            video_query = """CREATE TABLE IF NOT EXISTS videos(Channel_name VARCHAR(255),Channel_id VARCHAR(255), Video_id VARCHAR(255), Title TEXT, Thumbnail TEXT, Description TEXT, Published_date DATETIME, Views INT, Likes INT, Comments INT, Favorite_count INT, Definition VARCHAR(255), Caption_status VARCHAR(255))"""
            mycursor.execute(video_query)
            mydb.commit()

            for i in collections.find({"Channel_name" : user_input,}, {'_id':0, "Tags":0, "Duration":0 }):
                
                #CONVERTING DATETIME INTO SQL ACCEPTED FORMAT
                original_datetime_str = i['Published_date'] 
                original_datetime = datetime.strptime(original_datetime_str, '%Y-%m-%dT%H:%M:%SZ')
                formatted_datetime_str = original_datetime.strftime('%Y-%m-%d %H:%M:%S')
                i['Published_date'] = formatted_datetime_str

                t=tuple(i.values())
                mycursor.execute(query1,t)
                mydb.commit()

        def insert_into_comments():
            comment_query = """CREATE TABLE IF NOT EXISTS comments(Comment_id VARCHAR(255),
                                                         Video_id VARCHAR(255),
                                                         Comment_text TEXT,
                                                         Comment_author TEXT,
                                                         Comment_published_date DATETIME,
                                                         Like_count INT,
                                                         Reply_count INT
                                                                        )"""
            mycursor.execute(comment_query)
            mydb.commit()

            collections1 = db.video_details
            collections2 = db.comments_details
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"Channel_name" : user_input},{'_id' : 0}):
                for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):

                    #CONVERTING DATETIME INTO SQL ACCEPTED FORMAT
                    original_datetime_str = i['Comment_posted_date']
                    original_datetime = datetime.strptime(original_datetime_str, '%Y-%m-%dT%H:%M:%SZ')
                    formatted_datetime_str = original_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    i['Comment_posted_date'] = formatted_datetime_str

                    t=tuple(i.values())
                    print(t)
                    mycursor.execute(query2,t)
                    mydb.commit()

        if st.button("Submit"):
                
            insert_into_channels()
            insert_into_videos()
            insert_into_comments()
            st.success("SUCCESS....!!!")
            st.balloons()

#ANALYSIS PAGE:
if selected == "Analysis":
    
    st.write("#### :orange[Choose a question from below and lets find out a new pattern:]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    'What are the names of all the videos and their corresponding channels?',
    'Which channels have the most number of videos, and how many videos do they have?',
    'What are the top 10 most viewed videos and their respective channels?',
    'How many comments were made on each video, and what are their corresponding video names?',
    'Which videos have the highest number of likes, and what are their corresponding channel names?',
    'What is the total number of likes, and what are their corresponding video names?',
    'What is the total number of views for each channel, and what are their corresponding channel names?',
    'What are the names of all the channels that have published videos in the year 2022?',
    'Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == 'What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT Channel_name, Title AS Video_name  FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == 'Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name 
        AS Channel_Name, total_videos AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :orange[Number of :blue[_videos_] in each channel :]")

        figure = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(figure,use_container_width=True)
        
    elif questions == 'What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT Channel_name, Title AS Video_name ,  Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :orange[Top :blue[_10_] most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == 'How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT Title AS Video_name, Comments AS Comment_count, Channel_name
                            FROM videos
                            order by comment_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == 'Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name , Title AS Video_name, Likes AS Likes_Count 
                            FROM videos
                            ORDER BY Likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :orange[Top :blue[_10_] most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == 'What is the total number of likes, and what are their corresponding video names?':
        mycursor.execute("""SELECT Title, likes AS Likes_Count FROM videos ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == 'What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, Views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :range[Channels :blue[_vs_] Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == 'What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT Channel_name AS Channel_Name
                            FROM videos
                            WHERE Published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == 'Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT Channel_name AS Channel_Name,Video_id AS Video_ID,Comments AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        st.write("### :orange[Videos with most :blue[_comments_] :]")
        st.write(pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names))
        
    