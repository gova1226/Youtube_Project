#interact with the YouTube API
import googleapiclient.discovery
import googleapiclient.errors
import mysql.connector as db #connect to the MySQL database
import streamlit as st #create the Streamlit web app
import pandas as pd #data manipulation and analysis
from streamlit_option_menu import option_menu #create an option menu in the Streamlit sidebar
import time


# Api Connection using API from Google Developer Console
def api_connect(): #function to connect to the YouTube API
    api = "AIzaSyCETRqag9VubFoIOdNlIKs-xYWup0EpS6w" #API key from Google Developer Console
    api_service_name = "youtube" #Define the service name
    api_version = "v3" #Define the version
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api) #Call the function and store the result in the youtube variable
    return youtube #Returns the YouTube service object

youtube = api_connect() #Calls the api_connect function and assigns the returned YouTube service object to the variable youtube


# Connecting to MySQL database
mydb = db.connect(
    host="localhost",
    port="3306",
    user="root",
    password="GovaMoni@Kanali26",
    database="youtube_project")
mycursor = mydb.cursor() #Create a cursor object to execute MySQL queries


# Creating tables if they don't exist
def create_tables(): #creates the necessary tables in the MySQL database if they do not already exist. These tables will store channel, video, and comment data
    mycursor.execute('''CREATE TABLE IF NOT EXISTS Channel_Table (
                           Channel_Name varchar(255),
                           Channel_Id varchar(255),
                           Subscribers BIGINT,
                           View_Count BIGINT,
                           Playlist_id varchar(100),
                           Total_videos INT,
                           Description Text)''')

    mycursor.execute('''CREATE TABLE IF NOT EXISTS video_ids (
                           video_id VARCHAR(255))''')

    mycursor.execute('''CREATE TABLE IF NOT EXISTS Videoz_Table (
                           Channel_Name varchar(255),
                           Channel_Id varchar(255),
                           Video_id varchar(255),
                           Title varchar(255),
                           Views bigint,
                           Likes bigint,
                           Comments bigint,
                           Thumbnail varchar(255),
                           published_at varchar(100),
                           Duration varchar(100),
                           Description Text)''')

    mycursor.execute('''CREATE TABLE IF NOT EXISTS comment_table (
                           comment_Id VARCHAR(255),
                           video_Id VARCHAR(255),
                           comment_Text TEXT,
                           comment_Author VARCHAR(255),
                           comment_Published VARCHAR(255))''')

create_tables() #Executes the function to ensure the necessary tables are created in the MySQL database if they do not already exist


# Getting the required channel information
def get_channel_details(channel_id): #To fetch and store details about a specific YouTube channel identified by its channel_id
    #To send a request to the YouTube Data API to retrieve channel details
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id) #Specifies the parts of the channel resource that the API response will include. ID of the channel to retrieve
    response = request.execute() #Executes the request and stores the response

    for item in response.get('items', []): #Iterates over the items in the API response to process each channel's data
        data = {
            'Channel_Name': item['snippet']['title'],
            'Channel_Id': channel_id,
            'Subscribers': item['statistics']['subscriberCount'],
            'View_Count': item['statistics']['viewCount'],
            'Playlist_id': item['contentDetails']['relatedPlaylists']['uploads'],
            'Total_videos': item['statistics']['videoCount'],
            'Description': item['snippet']['description']
        } #Extracts relevant channel details from the API response and stores them in a dictionary named data
        channel_details = (data['Channel_Name'], data['Channel_Id'], data['Subscribers'], data['View_Count'], data['Playlist_id'], data['Total_videos'], data['Description']) #Prepares the extracted channel details as a tuple for insertion into the MySQL database
        insert_query = ("INSERT INTO Channel_Table (Channel_Name, Channel_Id, Subscribers, View_Count, Playlist_id, Total_videos, Description) VALUES (%s, %s, %s, %s, %s, %s, %s)") #Defines the SQL query to insert the channel details into the Channel_Table
        mycursor.execute(insert_query, channel_details) #Executes the SQL query with the prepared data
        mydb.commit() #Commits the transaction to save the changes in the database
    return data #Returns the extracted channel details as a dictionary


# Getting all video ids using the next page token as it is limited to only 50 videos
def get_channel_videos(channel_id): #To fetch and store all video IDs associated with a specific YouTube channel identified by its channel_id.
    video_ids = [] #Initializes an empty list video_ids to store video IDs
    request = youtube.channels().list(id=channel_id, part='contentDetails') #creates a request to the YouTube API to get the contentDetails part of the channel resource for the specified channel_id
    response1 = request.execute() #executes the API request and stores the response in the variable response1
    playlist_id = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads'] #extracts the playlist ID of the channel's uploads playlist from the response
    next_page_token = None #initializes a variable next_page_token to None. It will be used to handle pagination in the API requests

    while True: #starts an infinite loop, which will continue until there are no more pages of results
        request = youtube.playlistItems().list(playlistId=playlist_id, part='snippet', maxResults=50, pageToken=next_page_token)
        #youtube.playlistItems().list(...) - calls the YouTube Data API's playlistItems.list method
        #part="snippet" - specifies to retrieve the snippet part of the playlist item resource
        #playlistId=playlist_videos - specifies the playlist ID to retrieve videos from
        #maxResults=50 - limits the number of results returned in each API call to 50
        #pageToken=next_page_token - uses the next_page_token for pagination

        response2 = request.execute() #executes the API request and stores the response in the variable response2
        for i in range(len(response2['items'])): #iterates over each item in the response
            video_ids.append(response2['items'][i]['snippet']['resourceId']['videoId']) #appends the video ID to the videos_ids list
        next_page_token = response2.get('nextPageToken') #updates the next_page_token with the value from the response to prepare for the next page of results

        if next_page_token is None: #if there is no next_page_token, it means there are no more pages of results
            break #exits the while loop if there are no more pages

    batch_video_ids = [(video_id,) for video_id in video_ids] #Prepares the list of video IDs as a list of tuples for insertion into the MySQL database
    mycursor.executemany("INSERT INTO video_ids (video_id) VALUES (%s)", batch_video_ids) #Executes a bulk insert of the video IDs into the video_ids table
    mydb.commit() #Commits the transaction to save the changes in the database
    return video_ids #Returns the list of fetched video IDs

# Getting video details using the video ids
def get_video_details(video_ids): #defines a function named get_video_details which takes one parameter, video_ids a list of video IDs
    video_data = [] #Initializes an empty list named video_data to store the details of each video
    #Begins a try block to handle potential exceptions that might occur during the execution of the code within it
    try:
        for video_id in video_ids: #Starts a for loop to iterate over each video ID in the video_ids list

            #Constructs a request to the YouTube Data API to retrieve details about the video. 
            # The part parameter specifies that the response should include snippet, contentDetails, and statistics parts of the video resource, and the id parameter specifies the video ID
            request = youtube.videos().list(part="snippet, contentDetails, statistics", id=video_id)
            response = request.execute() #Executes the API request and stores the response in the response variable

            #Starts another for loop to iterate over each item in the items list of the response.
            # The get method is used to safely retrieve the items key, returning an empty list if items is not found
            for item in response.get('items', []): 
                data = {
                    'Channel_Name': item['snippet']['channelTitle'],
                    'Channel_Id': item['snippet']['channelId'],
                    'Video_id': item['id'],
                    'Title': item['snippet']['title'],
                    'Views': item['statistics'].get('viewCount', 0),
                    'Likes': item['statistics'].get('likeCount', 0),
                    'Comments': item['statistics'].get('commentCount', 0),
                    'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                    'published_at': item['snippet']['publishedAt'].replace('T', ' ').replace('Z', ''),
                    'Duration': item['contentDetails']['duration'].replace('PT', ' '),
                    'Description': item['snippet']['description']
                } #Creates a dictionary named data to store relevant details about the video. Each key-value pair in the dictionary extracts specific information from the item and formats it as needed
                video_data.append(data) #Appends the data dictionary to the video_data list

        #Creates a list comprehension that transforms each data dictionary in video_data into a tuple of its values, which are in the order expected by the SQL insert query
        batch_video_data = [(data['Channel_Name'], data['Channel_Id'], data['Video_id'], data['Title'], data['Views'], data['Likes'], data['Comments'], data['Thumbnail'], data['published_at'], data['Duration'], data['Description']) for data in video_data]
        
        #Defines an SQL insert query as a string. The placeholders %s will be replaced with actual values when executing the query
        insert_query = """INSERT INTO Videoz_Table (Channel_Name, Channel_Id, Video_id, Title, Views, Likes, Comments, Thumbnail, published_at, Duration, Description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        #Executes the SQL insert query for all the tuples in batch_video_data. executemany allows executing the same query multiple times with different data
        mycursor.executemany(insert_query, batch_video_data)
        mydb.commit() #Commits the transaction to the database, making the changes permanent

    #Catches any exceptions that occur during the execution of the try block and stores the exception in variable e & Prints an error message along with the exception details
    except Exception as e:
        print("Error:", e)
    return video_data #Returns the list video_data which contains all the video details fetched from the API

# Getting the comments details
def get_comments_details(video_ids): #Defines a function named get_comments_details which takes one parameter, video_ids, a list of video IDs
    comment_data = [] #Initializes an empty list named comment_data to store the details of each comment
    try: #Begins a try block to handle potential exceptions that might occur during the execution of the code within it
        for video_id in video_ids: #Starts a for loop to iterate over each video ID in the video_ids list
            try: #Begins a nested try block to handle potential exceptions that might occur specifically during the API request for each video ID

                #Constructs a request to the YouTube Data API to retrieve comments for the video. The part parameter specifies that the response should include the snippet part of the comment thread resource. 
                # The videoId parameter specifies the video ID, and maxResults=50 limits the response to 50 comments
                request = youtube.commentThreads().list(part='snippet', videoId=video_id, maxResults=50) 
                response = request.execute() #Executes the API request and stores the response in the response variable
                for item in response['items']: #Starts another for loop to iterate over each item in the items list of the response

                    #Creates a dictionary named data to store relevant details about the comment.
                    # Each key-value pair in the dictionary extracts specific information from the item and formats it as needed
                    data = {
                        'comment_Id': item['snippet']['topLevelComment']['id'],
                        'video_Id': item['snippet']['topLevelComment']['snippet']['videoId'],
                        'comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'comment_Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'comment_Published': item['snippet']['topLevelComment']['snippet']['publishedAt'].replace('T', ' ').replace('Z', '')
                    }
                    comment_data.append(data) #Appends the data dictionary to the comment_data list
            #Catches any HttpError exceptions that occur during the API request. 
            # If the error status is 403, it indicates that comments are disabled for the video, and a message is printed to notify the user. 
            # If the error is different, a generic error message is printed
            except googleapiclient.errors.HttpError as e:
                error = e.resp.status
                if error == 403:
                    print(f"Comments are disabled for video ID {video_id}. Skipping...")
                else:
                    print(f"An error occurred: {e}")


        #Creates a list comprehension that transforms each data dictionary in comment_data into a tuple of its values, which are in the order expected by the SQL insert query
        batch_comment_data = [(data['comment_Id'], data['video_Id'], data['comment_Text'], data['comment_Author'], data['comment_Published']) for data in comment_data]
        
        #Defines an SQL insert query as a string. The placeholders %s will be replaced with actual values when executing the query
        insert_query = """INSERT INTO comment_table (comment_Id, video_Id, comment_Text, comment_Author, comment_Published) VALUES (%s, %s, %s, %s, %s)"""
        
        #Executes the SQL insert query for all the tuples in batch_comment_data. executemany allows executing the same query multiple times with different data
        mycursor.executemany(insert_query, batch_comment_data)
        mydb.commit() #Commits the transaction to the database, making the changes permanent

    #Catches any exceptions that occur during the execution of the outer try block and stores the exception in variable e
    except Exception as e:
        print(f"Error: {e}") #Prints an error message along with the exception details

    return comment_data #Returns the list comment_data which contains all the comment details fetched from the API



# Function to call the channel information
def channel_info(channel_id): #Defines a function named channel_info which takes one parameter, channel_id, representing the ID of a YouTube channel
    
    #Calls the get_channel_details function with channel_id as an argument to retrieve the details of the specified channel and result is stored in the channel_details variable
    channel_details = get_channel_details(channel_id)
    
    #Calls the get_channel_videos function with channel_id as an argument to retrieve a list of video IDs from the specified channel and result is stored in the video_Ids variable
    video_Ids = get_channel_videos(channel_id)

    #Calls the get_video_details function with video_Ids as an argument to retrieve the details of the specified videos and result is stored in the video_details variable
    video_details = get_video_details(video_Ids)

    #Calls the get_comments_details function with video_Ids as an argument to retrieve the comments of the specified videos and result is stored in the comment_details variable
    comment_details = get_comments_details(video_Ids)

    #Creates a pandas DataFrame from the channel_details dictionary. 
    #The dictionary is enclosed in a list to ensure it's properly formatted as a single row DataFrame and result is stored in the channel_df variable
    channel_df = pd.DataFrame([channel_details])

    #Creates a pandas DataFrame from the video_details list of dictionaries.
    #Each dictionary in the list represents a row in the DataFrame and result is stored in the video_df variable
    video_df = pd.DataFrame(video_details)

    #Creates a pandas DataFrame from the comment_details list of dictionaries.
    #Each dictionary in the list represents a row in the DataFrame and result is stored in the comment_df variable
    comment_df = pd.DataFrame(comment_details)

#Returns a dictionary containing the three DataFrames: channel_df, video_df, and comment_df, under the keys "channel_details", "video_details", and "comment_details", respectively
    return {
        "channel_details": channel_df,
        "video_details": video_df,
        "comment_details": comment_df,
    }



# Codes to run the Streamlit application using if else.
with st.sidebar: #creates a context for the sidebar using the st.sidebar component from Streamlit. All elements inside this context will be placed in the sidebar
    #creates an option menu in the sidebar using the option_menu function
    opt = option_menu("Main Menu",
                      ["Home", "Data Collection and Upload", "Analysis using MySQL"],
                      icons=["house", "cloud-upload", "database", "filetype-sql"],
                      menu_icon="menu-up",
                      orientation="vertical")

if opt == "Home": #checks if the selected option from the sidebar menu is "Home"
    st.image('D:/Gova/Data Science Course/Projects/1.Youtube Datawarehouse/Youtube_logo.png', width=100) #Displays an image (YouTube logo) from the specified path with a width of 100 pixels
    st.title(''':red[YouTube] :blue[Data Harvesting & Warehousing using MySQL]''') #Sets the title of the page with the text "YouTube Data Harvesting & Warehousing using MySQL"
    st.subheader(':blue[Domain :] Social Media') #Adds a subheader with the text "Domain: Social Media" in blue color
    st.subheader(':blue[Overview :]')
    st.markdown('''Build a simple dashboard or UI using Streamlit and retrieve YouTube channel data with the help of the YouTube API. Stored the data in an MySQL database(warehousing) managed by MySQL Workbench, enabling querying of the data using MySQL. Visualize the data within the Streamlit app to uncover insights, trends with the YouTube channel data.''') #Markdown section describing the project overview
    st.subheader(':blue[Skill Take Away :]')
    st.markdown(''' Python scripting, Data Collection, API integration, Data Management using MySQL, Streamlit''')
    st.subheader(':blue[About :]')
    st.markdown('''Hello! My name is Govardhanan, and I'm a BE graduate in computer science. My first project in data science was gathering and warehousing YouTube data using MySQL. I extracted relevant insights from the large amount of data. This encounter fueled my interest in data-driven decision-making and enhanced my knowledge of data extraction techniques and database management.''')
    st.subheader(':blue[Contact:]')
    st.markdown('''+91 98942-27346''')
    st.subheader(':blue[Email:]')
    st.markdown('''govardhananprema@gmail.com''')
    st.subheader(':blue[LinkedIn:]')
    st.markdown('''www.linkedin.com/in/govardhanan-g-1b2770102''')

elif opt == "Data Collection and Upload": #checks if the selected option from the sidebar menu is "Data Collection and Upload"
    st.title(':blue[Data Collection and Upload]')
    st.markdown('''
                - Provide channel ID in the input field.
                - Then click 'Data Collection and Upload' will store the extracted channel, videos, comments information in MySQL Database''')
    st.markdown('''
                :red[NOTE:] ***How to get channel_ID: ***
                Open Youtube --> go to any Channel --> go to About --> Click on share channel --> Copy channel_ID''')
    st.subheader(":blue[Enter YouTube Channel ID]")
    channel_id = st.text_input('Channel_ID') #text input field where the user can enter the YouTube channel ID

    if st.button("Data Collection & Upload"): #Checks if the "Data Collection & Upload" button is clicked
        with st.spinner("Please wait, data collection and upload in progress..."): #Displays a spinner with the message "Please wait, data collection and upload in progress..." while the data collection and upload process is running

            #Tries to collect and upload the data using the channel_info function. 
            #If successful, it displays a success message. If an error occurs, it catches the exception and displays an error message with the exception details
            try:
                data = channel_info(channel_id)
                st.success('Data Successfully Collected & Uploaded to Database')
            except Exception as e:
                st.error(f"An error occurred: {e}")

elif opt == "Analysis using MySQL": #checks if the selected option from the sidebar menu is "Analysis using MySQL"
    st.title(":blue[Performing Analysis using MySQL Queries]")
    st.subheader(':blue[Below are some sample SQL Queries to use for analysis]')

    questions=st.selectbox("Select any question to get the Insights from Dropdown box",
                                    ["Choose your Questions...",
                                     '1.What are the names of the all videos and their corresponding channels?',
                                     '2. Which channels have the most number of videos, and how many videos do they have?',
                                     '3. What are the top 10 most viewed videos and their respective channels?',
                                     '4. How many comments were made on each video, and what are their corresponding video names?',
                                     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                     '8. What are the names of all the channels that have published videos in the year 2022?',
                                     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                     '10. Which videos have the highest number of comments, and what are their corresponding channel names?' ],
                                      index=0) #Streamlit function that creates a dropdown menu & sets the default selected option in the dropdown menu to the first item, which in this case is "Choose your Questions..."

    if questions == '1.What are the names of the all videos and their corresponding channels?': #Checks if the selected question is the first one
                  mycursor.execute("""SELECT Title as Title , Channel_Name as Channel_Name  FROM youtube_project.videoz_table ;""") #Executes an SQL query to retrieve the title and channel name of all videos
                  df = pd.DataFrame(mycursor.fetchall(), columns=['Title','Channel_Name']) #Fetches the results of the query and stores them in a pandas DataFrame
                  st.write(df) #Displays the DataFrame in the Streamlit app
      
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            mycursor.execute("""SELECT Channel_Name, COUNT(*) AS Video_Count FROM youtube_project.videoz_table GROUP BY Channel_Name ORDER BY Video_Count DESC;""")
            df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_Name', 'Video_Count'])
            st.write(df)

            
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            mycursor.execute("""SELECT Title, Channel_Name, Views FROM youtube_project.videoz_table ORDER BY Views DESC LIMIT 10""")
            data = mycursor.fetchall()
            df = pd.DataFrame(data, columns=['Title', 'Channel_Name', 'Views'])
            st.write(df)


    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            mycursor.execute("""select Channel_Name , Title ,Video_id, Comments from youtube_project.videoz_table ;""")
            df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_Name', 'Title','Video_id','Comments'])
            st.write(df)

              
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            mycursor.execute("""SELECT Channel_Name, Video_Id, Title, Likes FROM youtube_project.videoz_table v WHERE Likes = (SELECT MAX(Likes)
                                    FROM youtube_project.videoz_table WHERE Channel_Name = v.Channel_Name )""")
            data = mycursor.fetchall()
            df = pd.DataFrame(data, columns=['Channel_Name','Video_Id', 'Title', 'Likes'])
            st.write(df)


    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            mycursor.execute("""SELECT Title as Title , SUM(Likes) as Likes FROM youtube_project.videoz_table  GROUP BY Title""")
            df = pd.DataFrame(mycursor.fetchall(),columns=['Title', 'Likes'])
            st.write(df)


    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, View_Count AS Views FROM youtube_project.channel_table ORDER BY Views DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)


    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
            mycursor.execute("""SELECT Channel_Name AS Channel_Name FROM videoz_table WHERE published_at LIKE '2022%' GROUP BY Channel_Name ORDER BY Channel_Name""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)


    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            mycursor.execute("""SELECT Channel_Name AS Channel_Name,AVG(duration)/60 AS "Average_Video_Duration (mins)" FROM youtube_project.videoz_table
                                GROUP BY Channel_Name ORDER BY AVG(Duration)/60 DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)


    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
           mycursor.execute(""" SELECT Channel_Name, Title, Comments FROM youtube_project.videoz_table v WHERE Comments = (
                                        SELECT MAX(Comments) FROM youtube_project.videoz_table WHERE Channel_Name = v.Channel_Name); """)
           data = mycursor.fetchall()
           df = pd.DataFrame(data, columns=['Channel_Name','Title','Comments'])
           st.write(df)
