YouTube-Data-Harvesting-and-Warehousing-using-MySQL-and-Streamlit

**INTRODUCTION:**
* The project about Building a simple dashboard or UI using Streamlit.
* Retrieve YouTube channel data with the help of Google API.
* Stored the data in MySQL database,with the help of MySQL workbench.
* Enabling querying of the data using MySQL and Visualize the data within the Streamlit.

**DOMAIN:** 
* Social Media

**SKILLS TAKEAWAY:**
* Python scripting
* Data Collection
* Streamlit
* API integration
* Data Management using MySQL

**OVERVIEW**
**DATA HARVESTING:**
* Utilizing the YouTube API to collect data such as channel information, comments, and video details.

**DATA STORAGE:**
* Setting up a local MySQL database using MySQL workbench.
* Creating tables to store the harvested YouTube data.
* Using SQL scripts to insert the collected data into the database.

**DATA ANALYSIS and VISUALIZATION:**
* Developing a Streamlit application to interact with the MySQL database.
* Creating visualizations and performing analysis on the stored YouTube data

**TECHNOLOGY AND TOOLS**
Python 3.12.1
MYSQL
Google API
Streamlit

📚 Packages and Libraries
google-api-python-client
👉 import googleapiclient.discovery
👉from googleapiclient.errors import HttpError
mysql-connector-python
👉 import mysql.connector
SQLAlchemy
👉 from sqlalchemy import create_engine
pandas
👉 import pandas as pd
streamlit
👉 import streamlit as st
streamlit_option_menu
👉 from streamlit_option_menu import option_menu
plotly
👉 import plotly.express as px
pillow
👉 from PIL import Image

**DATA COLLECTION:**
The data collection process involved retrieving various data points from YouTube using the YouTube Data API. Retrieve channel information, videos details, playlists and comments.

💾 Database Storage:
*The collected YouTube data was transformed into pandas dataframes. Before that, a new database and tables were created using the XAMPP control panel. With the help of SQLAlchemy, the data was inserted into the respective tables. The database could be accessed and managed in the MySQL environment provided by XAMPP.

📋Data Analysis:
By using YouTube channel data stored in the MySQL database, performed MySQL queries to answer 10 questions about the YouTube channels. When selecting a question, the results will be displayed in the Streamlit application in the form of tables.
📊 Data Visualization:
By using YouTube channel data stored in the MySQL database, The data was presented in visually appealing charts and graphs using Plotly. when selecting a Query, the visualization diplayed in streamlit application

📘 Usage
Enter a YouTube channel ID or name in the input field in Data collection option from sidebar menu.
Click the "View Details" button to fetch and display channel information.
Click the "Upload to MySQL" button to store channel data in the SQL database.
Select Analysis and Visualization options from the sidebar menu to analyze and visualize data.
