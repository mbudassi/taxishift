from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import os, sys, boto,time

#We are searching for 10 hour shift offenders after this time, which we call zero-hour:
zerohour = time.mktime(time.strptime("2013-04-09 15:00:00","%Y-%m-%d %H:%M:%S"))

#Parse raw csv data to create key-value pairs
def splitit(x):

    #Ignore heading of data
    i = x.strip().split(',')
    if i[0] == 'medallion':
        return (0,[0,0])

    #Extract pickup and dropoff times
    a = time.mktime(time.strptime(i[5],"%Y-%m-%d %H:%M:%S"))
    b = time.mktime(time.strptime(i[6],"%Y-%m-%d %H:%M:%S"))

    #Return Key-value pair
    return (i[1],[a,b])

#Merge rides to form shifts
def mergeit(x):

    i = 1
    x.sort()

    #If the time between rides is under an hour, merge them to create shift
    #Ignore rides ten hours before zero-hour 
    while i < len(x):
        if x[i-1][1] < (zerohour - 36000):
            x.pop(i-1)
        elif x[i][0] - x[i-1][1] < 3600:
            x[i][0] = x[i-1][0]
            x.pop(i-1)
        else:
            i+=1

    return x

def main(*argv):

    #Setup to read from s3
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

    conf = SparkConf().setAppName("taxishift2")
    SparkContext.setSystemProperty('spark.executor.memory', '5g')
    sc = SparkContext(conf=conf)
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId",aws_access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey",aws_secret_access_key)
    
    #Check for new data every five minutes
    ssc = StreamingContext(sc, 300)
    ssc.checkpoint('s3a://ddrum-s3/checkpoint/')

    #Update state with new data
    def updateFunc(currentTaxi, taxiState):
        if taxiState is None:
            taxiState = []

        #Create total list of rides/shifts for each driver
        return mergeit(currentTaxi + taxiState)

    #Read streaming from s3
    raw_data = ssc.textFileStream('s3a://ddrum-s3/trip_data/')

    #Call user defined function to map raw data into key-value pairs, and update state
    total_data = raw_data.map(lambda x:splitit(x))\
                         .updateStateByKey(updateFunc)

    #Create key-value for each shift, and return people with nine hour shifts or greater within 30 minutes of zerohour
    warn_list = total_data.flatMap(lambda x: [(x[0],r) for r in x[1]])\
                          .filter(lambda x:x[1][1] - x[1][0] > 32400)\
                          .filter(lambda x:x[1][1] < (zerohour + 1800) and x[1][1] > zerohour)
    
    #Indicator warning
    warn_list.pprint()
    
    #Begin stream
    ssc.start()
    ssc.awaitTermination()

    return

if __name__=="__main__":
    main(*sys.argv)
