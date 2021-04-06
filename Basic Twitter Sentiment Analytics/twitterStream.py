from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    countPositive = []
    countNegative = []
    for line in counts:
        for w in line:
            if w[0]=="positive":
                countPositive.append(w[1])
            else:
                countNegative.append(w[1])
                
    positive, = plt.plot(countPositive,'go--')
    negative, = plt.plot(countNegative,'bo--')
    plt.xlabel('Timestep')
    plt.ylabel('Word Count')
    plt.legend((positive, negative),('Positive','Negative'), loc = 2)
    plt.show();
    plt.savefig('sentimentPlot.png')
    
              
                                    
            
           
         
def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    wordlist = []
    file = open(filename,'r')
    for word in file:
        wordlist.append(word.split("\n")[0])
    return set(wordlist)
    


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    tweets = tweets.flatMap(lambda row: row.split(" ")).filter(lambda x : (x in pwords) or (x in nwords)).map(lambda x:('positive',1) if (x in pwords) else('negative',1)).reduceByKey(lambda p,n : p+n)
                                                                                                            
    words_sum = tweets.updateStateByKey(calculateSum)
    words_sum.pprint()                                                                                                          
                                                                                                              
                                                                                                              
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    tweets.foreachRDD(lambda row, rdd:counts.append(rdd.collect()))
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


def calculateSum(value1, value2):
    if value2 is None:
        value2 = 0
    result = sum(value1, value2)
    return result

if __name__=="__main__":
    main()
