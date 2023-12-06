from googleapiclient.discovery import build
import pymongo
import pandas as pd
import psycopg2
import streamlit as st

#APi key connecxtion 

def Api_connect():
    Api_Id="AIzaSyCf_84yxiG2GoKtbdy8f12YNxNZVayorjI"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube=Api_connect()
#APi key connecxtion 

# get channel info
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                  Channel_Id=i['id'],
                  Subscriber_Count=i['statistics']['subscriberCount'],
                  Channel_Views=i['statistics']['viewCount'],
                  Total_videos=i['statistics']['videoCount'],
                  Channel_Description=i['snippet']['description'],
                  Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
                 )
    return data

#get video ids
def get_Video_Ids(channel_id):
    
    video_Ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()

    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1= youtube.playlistItems().list(
                                          part='snippet',
                                          playlistId=Playlist_Id,
                                          maxResults=50,
                                          pageToken=next_page_token).execute()
    #for loop executed for 50 items appended in video_Ids list
        for i in range(len(response1['items'])):
            video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
    # for next 50 items get next page token from page token in snippet   
        if next_page_token is None:
            break
    # if next page token is none then loop will break 
        #next_page_token=response1['nextPageToken']
    return video_Ids

#get video information video id
def get_video_info(Video_Ids):
    video_data=[]
    for video_id in Video_Ids:
        request=youtube.videos().list(
            part= "snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response ['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                      Channel_Id=item['snippet']['channelId'],
                      Video_Id=item['id'],
                      Video_name=item['snippet']['title'],
                      Tags=item['snippet'].get('tags'),
                      Thumbnails=item['snippet']['thumbnails']['default']['url'],
                      Video_Description=item['snippet'].get('description'),
                      PublishedAt=item['snippet']['publishedAt'],
                      Duration=item['contentDetails']['duration'],
                      Views_Count=item['statistics']['viewCount'],
                      like_Count=item["statistics"]['likeCount'],
                      Comments_Count=item['statistics'].get('commentCount'),
                      favorite_Count=item['statistics']['favoriteCount'],
                      Caption_Status=item['contentDetails']['caption']
                     )
            video_data.append(data)
    return video_data

#get comment info
#if comment section disable use try and except to avoid error

def get_comment_info(Video_Ids):
    comment_data=[]
    try:
        for video_id in Video_Ids:
            request=youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50
                        )
            response = request.execute()
            for item in response['items']:
                data=dict(Comment_id=item['snippet']['topLevelComment']['id'],
                          Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_published_data=item['snippet']['topLevelComment']['snippet']['publishedAt']  
                          )
                comment_data.append(data)
    except:
        pass
    return comment_data

# get playlist details
def get_playlist_details(channel_id):

    next_page_token=None
    Playlist_data=[]
    while True:
        request=youtube.playlists().list(
                                part="snippet,contentDetails",
                                channelId=channel_id,
                                maxResults=50,
                            
            pageToken=next_page_token
                                )
        response = request.execute()
        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                      Channel_Id=item['snippet']['channelId'],
                      Playlist_Title=item['snippet']['title'],
                      Channel_Name=item['snippet']['channelTitle'],
                      PublishedAt=item['snippet']['publishedAt'],
                      Video_Count=item['contentDetails']['itemCount']
                       )
            Playlist_data.append(data)
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return Playlist_data

mongodb_url = 'mongodb://localhost:27017/'
database_name='Youtube_Data'
client=pymongo.MongoClient(mongodb_url)
db=client[database_name]


def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_Video_Ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1=db['channel_details']
    coll1.insert_one({'channel_information':ch_details,
                      'playlist_information':pl_details,
                      'video_information':vi_details,
                      'comment_information':com_details                    
                    })
    return 'upload completed successfully'


#table creation for channels,playlist,videos,comments
def channel_table():
    mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="post",
                        database="youtube_data",
                        port="5432")


    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(80) primary key,
                                                                Subscriber_Count bigint,
                                                                Channel_Views bigint,
                                                                Channel_Description text,
                                                                Total_videos int,
                                                                Playlist_Id varchar (80))'''
        cursor.execute(create_query)
        mydb.commit()   

    except:
        print("channels table already created")

    ch_list=[]
    db=client[database_name]
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}): # slicing only channel info
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscriber_Count,
                                            Channel_Views,
                                            Total_videos,
                                            Channel_Description,
                                            Playlist_Id)

                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscriber_Count'],
                row['Channel_Views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channel values are already inserted")

    
def playlist_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="post",
                            database="youtube_data",
                            port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists( Playlist_id varchar(100) primary key,
                                                                Playlist_Title varchar(100),
                                                                Channel_Name varchar(100),
                                                                Channel_Id varchar(100),
                                                                PublishedAt timestamp,    
                                                                Video_Count int
                                                                )'''
                                                            
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channels table already created")

    pl_list=[]
    db=client[database_name]
    coll1=db['channel_details']

    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)
    


    for index, row in df1.iterrows():
        insert_query = '''insert into playlists(Playlist_id,
                                                Playlist_Title,
                                                Channel_Name,
                                                Channel_Id,
                                                PublishedAt,
                                                Video_Count) 
                                                values(%s, %s, %s, %s, %s, %s)'''

        values =   (row['Playlist_Id'],
                    row['Channel_Id'],
                    row['Playlist_Title'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channel values are already inserted")    
        
        

#video
def videos_table():
    mydb = psycopg2.connect(host="localhost",
                                user="postgres",
                                password="post",
                                database="youtube_data",
                                port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(80) primary key,
                                                    Video_name varchar(100),
                                                    Tags text,
                                                    Thumbnails varchar(200),
                                                    Video_Description text,
                                                    PublishedAt timestamp,
                                                    Duration interval,
                                                    Views_Count bigint,
                                                    like_Count bigint,
                                                    Comments_Count int,
                                                    favorite_Count int,
                                                    Caption_Status varchar(100)                                             
                                                    )'''
                                                                                                                
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client[database_name]
    coll1=db['channel_details']
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
            
            insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Video_name,
                                                    Tags,
                                                    Thumbnails,
                                                    Video_Description,
                                                    PublishedAt,
                                                    Duration,
                                                    Views_Count,
                                                    like_Count,
                                                    Comments_Count,
                                                    favorite_Count,
                                                    Caption_Status)
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Video_name'],
                    row['Tags'],
                    row['Thumbnails'],
                    row['Video_Description'],
                    row['PublishedAt'],
                    row['Duration'],
                    row['Views_Count'],
                    row['like_Count'],
                    row['Comments_Count'],
                    row['favorite_Count'],
                    row['Caption_Status'])
            
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print("channel values are already inserted")    
        
        
def comments_tables():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="post",
                            database="youtube_data",
                            port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists comments( Comment_id varchar(100) primary key,
                                                                Video_id varchar(100),
                                                                Comment_text text,
                                                                Comment_author varchar(100),
                                                                Comment_published_data timestamp    
                                                                )'''
                                                                
                                                            
                                                                
                                                            
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channels table already created")

    com_list=[]
    db=client[database_name]
    coll1=db['channel_details']

    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3=pd.DataFrame(com_list)



    for index, row in df3.iterrows():
        insert_query = '''insert into comments(Comment_id,
                                                Video_id,
                                                Comment_text,
                                                Comment_author,
                                                Comment_published_data) 
                                                values(%s,%s,%s,%s,%s)'''

        values = (
            row['Comment_id'],
            row['Video_id'],
            row['Comment_text'],
            row['Comment_author'],
            row['Comment_published_data']
                )
        
        cursor.execute(insert_query,values)
        mydb.commit()
    
def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_tables()
    return "Successfully Channel Data Transferd to SQL"

def show_channel_table(): 
    ch_list=[]
    db=client[database_name]
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}): # slicing only channel info
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)
    
    return df

def show_videos_table():
    vi_list=[]
    db=client[database_name]
    coll1=db['channel_details']
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i])
    df2=st.dataframe(vi_list)
    return df2

def show_playlist_table():
    pl_list=[]
    db=client[database_name]
    coll1=db['channel_details']

    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data['playlist_information'])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)
    
    return df1

def show_comments_tables():
    com_list=[]
    db=client[database_name]
    coll1=db['channel_details']

    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df3=st.dataframe(com_list)
    return df3

# stream lit 
st.title(":red[YouTube Data Harvesting and Warehousing]")
with st.sidebar:
    st.title(":red[YouTube Data Harvesting and Warehousing]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id=st.text_input("Enter the channel ID")

if st.button("Collect and Store data"):
    ch_ids=[]
    db=client[database_name]
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}): # slicing only channel info
        ch_ids.append(ch_data['channel_information']['Channel_Id'])
        
    if channel_id in ch_ids:
        st.success("Channel Details of given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)
    
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)
    
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel_table()
    
elif show_table=="PLAYLISTS":
    show_playlist_table()
    
    
elif show_table=="VIDEOS":
    show_videos_table()
    
    
elif show_table=="COMMENTS":
    show_comments_tables()
    
mydb = psycopg2.connect(host="localhost",
                    user="postgres",
                    password="post",
                    database="youtube_data",
                    port="5432")
cursor=mydb.cursor()

Question = st.selectbox("Select Your Question",("1. What are the names of all the videos and their corresponding channels?",
                                                "2. Which channels have the most number of videos, and how many videos do they have?",
                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                "5. Which videos have the highest number of likes and what are their corresponding channel names?",
                                                "6. What is the total number of likes for each video, and what are their corresponding video names?",
                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8. What are the names of all the channels that have published videos in the year 2022?",
                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

if Question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select Video_name as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=['Video Title','Channel Name'])
    st.write(df)

elif Question=="2. Which channels have the most number of videos, and how many videos do they have?":
     query2='''select channel_name as channelname,total_videos as no_videos from channels
                order by total_videos desc'''
     cursor.execute(query2)
     mydb.commit()
     t2=cursor.fetchall()
     df2=pd.DataFrame(t2,columns=['Channel Name','No of Videos'])
     st.write(df2)

elif Question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''SELECT Views_Count, Channel_Name AS Channel_Name, Video_name AS Video_name
            FROM videos WHERE Views_Count IS NOT NULL ORDER BY Views_Count DESC LIMIT 10'''

    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=['Views','Channel Name','Video_Title'])
    st.write(df3)

elif Question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4='''SELECT comments_count as no_comments, video_name AS Video_Title FROM videos 
            WHERE comments_count IS NOT NULL '''

    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=['No of Comments','Video_Title'])
    st.write(df4)

elif Question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''SELECT video_name AS Video_Title, Channel_name AS Channel_Name,like_count AS Like_Count
                    FROM videos ORDER BY like_count DESC'''

    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=['Video_Title','Channel Name','Like_Count'])
    st.write(df5)
elif Question=="6. What is the total number of likes for each video, and what are their corresponding video names?":
    query6='''SELECT video_name AS Video_Title, like_count AS Like_Count FROM videos'''

    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=['Video_Title','Like_Count'])
    st.write(df6)

elif Question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_views as channel_views, channel_name as channel_name from channels'''

    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=['Total_No_Channel_Views','Channel_Name'])
    st.write(df7)

elif Question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select video_name as video_names,publishedat as video_release,channel_name as channel_names
                from videos where extract(year from publishedat)=2022'''

    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=['Video_Title','video_Published_ in 2022','Channel_Name'])
    st.write(df8)
elif Question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channel_names, duration as Avg_Duration from videos  order by channel_name'''

    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=['Channel_Name','Avg_Duration'])
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row['Channel_Name']
        average_duration=row['Avg_Duration']
        average_duration_str=str(average_duration)
        T9.append(dict(ChannelTitle=channel_title, Avgduration=average_duration_str))
    Df=pd.DataFrame(T9)
    st.write(Df)

elif Question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select video_name as title, comments_count as Comments_count, channel_name as channel_name 
                from videos order by Comments_count desc'''

    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=['Video_Name', 'Comments_Count','Channel_Name'])
    st.write(df10)




