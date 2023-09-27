# Twitter Data Analysis with Spark Streaming

### Project Summary 
This project focuses on processing and analyzing live data streams using Spark’s streaming APIs and Python.


### Streaming Architecture 
<img src="https://github.com/vvvvveraliu/Spark-TweeterStreaming/blob/main/Architecture.png" width="500" height="300" />

The project architecture is as above. A socket requests data from Twitter API and sends data to the Spark streaming process. Spark reads real-time data to do analysis. It also saves temp 
streaming results to Google Storage. After the streaming process terminates, Spark reads the final data from Google Storage and saves it to BigQuery, and then cleans the data in Storage. 
Finally, we use LDA to classify the data in the streaming.


### Tasks: 
1. Connecting to Twitter Stream to get tweets
* [This Script](https://github.com/vvvvveraliu/TwitterAnalysis-SparkStreaming-Python/blob/main/sparkStreaming.py) is responsible for getting the tweets from Twitter API using Python and
  passes them to the Spark Streaming processing using socket. It acts like a client of twitter API and a server of spark streaming. It open a listening TCP server socket, and listen to
  any connection from TCP client. After a connection established, it send streaming data to it.

2. Building Spark Streaming Application
* [This Script](https://github.com/vvvvveraliu/TwitterAnalysis-SparkStreaming-Python/blob/main/twitterHTTPClient.py) is spark streaming analysis process. It saves the outputs to
  BigQuery with the following tasks:
  * hashtagCount - Calculate the accumulated hashtags count sum from the beginning of the stream and sort it by descending order of the count
   * wordCount - Calculte the count of 5 sepcial words for every 60 seconds
     
* Resulting tables as follow:

<img src="https://github.com/vvvvveraliu/Spark-TweeterStreaming/blob/main/HashtagTable.png" width="300" height="400" />

<img src="https://github.com/vvvvveraliu/Spark-TweeterStreaming/blob/main/wordCount.png" width="300" height="400" />


3. Perform clustering on Stream data by LDA
* [This Script](https://github.com/vvvvveraliu/TwitterAnalysis-SparkStreaming-Python/blob/main/LDA.ipynb) uses PySpark's LDA (Latent Dirichlet Allocation) algorithm to perform topic
  modeling on the Stream Data. The output will include a "topicDistribution" column that represents the weight of each topic. Each row will have a distribution of values
  across the 10 topics, indicating the degree to which each topic is presented.

* Additionally, the LDA model is evaluated using the following two metrics:
   * Log Likelihood -   measure of how well the LDA model explains the observed data. A higher log likelihood indicates that the model is a better fit for the data.
   * Log Perplexity - measure of how well the LDA model generalizes to unseen data. Lower log perplexity values suggest that the model generalizes better to new data.
 



 


