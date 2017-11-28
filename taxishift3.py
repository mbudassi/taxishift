from pyspark import SparkContext, SparkConf
import os, sys, boto,time

#Parse raw csv data to create key-value pairs
def splitit(x):

    i = x.strip().split(',')
    if i[0] == 'medallion':
        return ((0,0),[[0,0]])

    #Extract pickup times
    a = time.mktime(time.strptime(i[5],"%Y-%m-%d %H:%M:%S"))

    #Extract pickup coordinates, and round to three decimal places (Further specificity unneeded)
    c = "%.3f" %  (float(i[10]))
    d = "%.3f" %  (float(i[11]))

    #Return key-value pair
    return ((c,d),[[a,a]])

#Merge pickup times to form pickup chains
def mergeit(x):

    i = 1
    x.sort()

    #Total pickups
    y = len(x)

    #If the time between pickups is under five minutes, merge them to create a pickup chain
    while i < len(x):
        if x[i][0] - x[i-1][1] < 300:
            x[i][0] = x[i-1][0]
            x.pop(i-1)
        else:
            i+=1

    #Return total pickup chains and total pickups
    return [len(x),y]


def main(*argv):

    #Setup to read from s3
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

    conf = SparkConf().setAppName("taxishift3")
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

    #Create total list of pickup chains and pickups for each coordinate
    #If there are more than three times as many pickups as pickup chains, this might be a taxistand
    total_data = total_data.reduceByKey(lambda x,y:x+y)\
                           .mapValues(lambda x:mergeit(x))\
                           .filter(lambda x:x[1][1]/x[1][0] > 3)\
                           .collect()

    #Print results
    total_data.sort()
    for i in total_data:
        print i

    return

if __name__=="__main__":
    main(*sys.argv)
