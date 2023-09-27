# Twitter Data Analysis with Spark Streaming

### Project Summary 
This project focuses on processing and analyzing live data streams using Spark’s streaming APIs and Python.


### Streaming Architecture 
<img src="https://github.com/vvvvveraliu/Spark-TweeterStreaming/blob/main/Architecture.png" width="500" height="300" />

The projectr architecture is as above. A socket requests data from Twitter API and sends data to the Spark streaming process. Spark reads real-time data to do analysis. It also saves temp 
streaming results to Google Storage. After the streaming process terminates, Spark reads the final data from Google Storage and saves it to BigQuery, and then cleans the data in Storage. 
Finally, we use LDA to classify the data in the streaming.


### Tasks: 
1. Connecting to Twitter Stream to get tweets
  * [Script]() is responsible for getting the tweets from Twitter API using Python and passes them to the Spark Streaming processing using socket. It acts like a client of
    twitter API and a server of spark streaming. It open a listening TCP server socket, and listen to any connection from TCP client. After a connection established, it send
    streaming data to it.

2. Building Spark Streaming Application
  * [Script]() is spark streaming analysis process. It saves the outputs to BigQuery with the following tasks: 
        * hashtagCount - Calculate the accumulated hashtags count sum from the beginning of the stream and sort it by descending order of the count
        * wordCount - Calculte the count of 5 sepcial words for every 60 seconds

 


