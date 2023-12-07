# YouTube Data Harvesting and Warehousing

## Overview

This project is a simple yet effective solution for harvesting data from YouTube and storing it in a centralized warehouse for analysis, reporting, or further processing. The YouTube API is utilized for extracting relevant information from videos, channels, and playlists, and the data is then stored in a database for easy access and retrieval.

PYTHON: Python is a high-level programming language that is designed to improve the readability of code. Python is the primary programming language used in the project for retrieving, transferring, and analyzing data.

GOOGLE API: The Google API Client Library for Python is designed for Python client-application developers and offers simple and flexible access to many Google APIs.The Google API v3 is the primary library that is used to retrieve data such as playlists, comments, and videos from YouTube channels.

MongoDB: MongoDB is a document database with the scalability and flexibility that you want with the querying and indexing that you need which is classified as NoSQL.In the project, it is the primary database to store data which was retrieved from the YouTube channel. It supports JSON format to store data.

POSTGRESQL: PostgreSQL is an advanced relational database system. It supports both relational (SQL) and non-relational (JSON) queries. In this project, it is the secondary database that gets data from MongoDB.Pre-set queries are created and displayed in web applications for user ease.

Streamlit: Streamlit turns data scripts into shareable web apps in minutes. All in pure Python. It is the primary web application used in the project. It is the Python library which can be installed in Python.

REQUIRED LIBRARIES:

1.googleapiclient.discovery

2.streamlet

3.psycopg2

4.pymongo

5.pandas

Feature of the project:

By getting the channel ID input from the user the YouTube channel data will be retrieved. Data such as Playlists,Comments,Videos and channel details.
Store channel data in MongoDB.
Transfering data from MongoDB to SQL
Sorting data according to usercase thru streamlit using a pre-set search option.
