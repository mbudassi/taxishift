from pyspark import SparkContext, SparkConf
import os, sys, boto,time
import matplotlib.pyplot as plt

#Parse raw csv data to create key-value pairs
def splitit(x):

    #Ignore heading of data
    i = x.strip().split(',')
    if i[0] == 'medallion':
        return (0,[[0,0]])

    #Extract pickup and dropoff times
    a = time.mktime(time.strptime(i[5],"%Y-%m-%d %H:%M:%S"))
    b = time.mktime(time.strptime(i[6],"%Y-%m-%d %H:%M:%S"))

    #Return Key-value pair
    return (i[1],[[a,b]])

#Merge rides to form shifts
def mergeit(x):

    i = 1
    x.sort()

    #If the time between rides is under an hour, merge them to create shift
    while i < len(x):
        if x[i][0] - x[i-1][1] < 3600:
            x[i][0] = x[i-1][0]
            x.pop(i-1)
        else:
            i+=1

    return x

def main(*argv):

    #Setup to read from s3
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

    conf = SparkConf().setAppName("taxishift1")
    SparkContext.setSystemProperty('spark.executor.memory', '5g')
    sc = SparkContext(conf=conf)
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId",aws_access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey",aws_secret_access_key)

    #Read from s3
    for i in range(1,13):

        raw_data = sc.hadoopFile('s3a://ddrum-s3/trip_data/trip_data_' + str(i) + '.csv',\
                                 'org.apache.hadoop.mapred.TextInputFormat',\
                                 'org.apache.hadoop.io.Text',\
                                 'org.apache.hadoop.io.LongWritable')

        #Call user defined function to map raw data into key-value pairs 
        new_data = raw_data.map(lambda x:splitit(x[1]))

        #Combine data from multiple csv files
        if i<2:
            total_data = new_data
        else:
            total_data = total_data.union(new_data)

        #Create total list of rides/shifts for each driver
        total_data = total_data.reduceByKey(lambda x,y:x+y)\
                               .mapValues(lambda x:mergeit(x))\
    
    #Create key-value for each shift, and set shifts greater than 10 hours to 10 hours (We want to know exactly when they go over)
    ungrouped_data = total_data.flatMap(lambda x: [(x[0],r) for r in x[1]])\
                               .mapValues(lambda x: x if x[1]-x[0] < 36000 else [x[0],x[0]+36000]) 

    #Extract only 10 hour shift offenders
    offenders = ungrouped_data.filter(lambda x:x[1][1] - x[1][0] >= 36000)\
                              .map(lambda x:(x[1][1]))\
                              .collect()

    #Plot number of offenders for every 30 minutes
    offender_hist = plt.hist(offenders, bins=range(int(min(offenders)), int(max(offenders)) + 1800, 1800))

    hist_min = int(min(offender_hist))
    hist_min+=900

    #Save to file
    hist_csv  = open('/home/ubuntu/offenders.csv','w')

    for i in offender_hist[0]:
        hist_csv.write(str(hist_min)+','+str(i)+'\n')
        hist_min+=1800
    hist_csv.close()

    return

if __name__=="__main__":
    main(*sys.argv)
