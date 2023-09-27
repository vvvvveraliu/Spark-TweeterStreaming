# Twitter Data Analysis with Spark Streaming

### Project Summary 
This project 

### Streaming Architecture 
The architecture is as follows. A socket requests data from Twitter API and sends data to the Spark streaming process. Spark reads real-time data to do analysis. It also saves temp 
streaming results to Google Storage. After the streaming process terminates, Spark reads the final data from Google Storage and saves it to BigQuery, and then cleans the data in Storage. 
Finally, you will try how to use LDA to classify the data in the streaming.
<img src="(https://github.com/vvvvveraliu/Spark-TweeterStreaming/blob/main/Architecture.png" width="300" height="200" />


### Tasks: 
1. Calculate the accumulated hashtags count sum for 600 seconds and sort it by descending order of the count. Hashtag usually starts with "#" followed by a series of alphanumeric and are
   usually case insensitive.
2. 




