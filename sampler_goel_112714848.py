##########################################################################
## Simulator.py  v 0.1
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: Rishabh Goel
## Student ID: 112714848

##Data Science Imports: 
import numpy as np
import mmh3
from random import random


##IO, Process Imports: 
import sys
from pprint import pprint

#Additional libraries
import random
import time


# helper function to prevent sum overflow by using means,std
# of uniform bucket calculated so far and the
# sum of last (if there is one) non-uniform bucket
def calculate(n,mod,sum1,sum2,sqsum1,sqsum2):
    if n%mod !=0 :
        sum1=sum1/(n%mod)
        sqsum1 = sqsum1/(n%mod)
    if n/mod !=0 :
        sum2=sum2/(n/mod)
        sqsum2 = sqsum2/(n/mod)
    mean = sum1*(n%mod/(n)) + sum2*((mod*(n/mod))/n)
    meansq = sqsum1*(n%mod/(n)) + sqsum2*((mod*(n/mod))/n)
    standard_deviation = pow(meansq - pow(mean,2),1/2)
    return mean,standard_deviation


##########################################################################
##########################################################################
# Task 1.A Typical non-streaming multi-level sampler

def typicalSampler(filename, percent = .01, sample_col = 0):
    # Implements the standard non-streaming sampling method
    mean, standard_deviation = 0.0, 0.0
    
    # Step 1: read file to pull out unique user_ids from file
    unique_ids = set()
    for row in filename:
        unique_ids.add(row.split(',')[sample_col])

    # Step 2: subset to random  1% of user_ids
    user_sample = random.sample(unique_ids,int(len(unique_ids)*percent))

    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn
    n = 0
    sum1, sqsum1 = 0.0,0.0
    sum2, sqsum2 = 0.0,0.0
    mod=10
    filename.seek(0)
    for row in filename:
        record = row.split(',')
        uid = record[sample_col]
        if uid in user_sample:
            n=n+1
            transaction = float(record[3])
            
            sum1=sum1+transaction
            sqsum1 = sqsum1 + pow(transaction,2)

            # to prevent overflow,calculating means of buckets,
            # increasing bucket size by multiple of 10
            # after we have 10 elements of size of prev bucket
            if(n%mod==0):
                sum2=sum2+sum1/mod
                sqsum2=sqsum2+sqsum1/mod
                sum1=0
                sqsum1=0
            if(n%(mod*10)==0):
                sum2=sum2/10
                sqsum2=sqsum2/10
                mod=mod*10
                sum1=0
                sqsum1=0

    mean,standard_deviation = calculate(n,mod,sum1,sum2,sqsum1,sqsum2)
    ##<<COMPLETE>>

    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.B Streaming multi-level sampler

def streamSampler(stream, percent = .01, sample_col = 0):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed. 
    
    mean, standard_deviation = 0.0, 0.0

    #calculating number of buckets out of which we will choose a random bucket
    # for eg , for 2%, we choose 1 out of 50, for 0.5%, we choose 1 out of 200
    buckets = int(100.0/(100.0*percent))
    
    random_bucket = int(random.random()*buckets)

    n=0
    sum1, sqsum1 = 0.0,0.0
    sum2, sqsum2 = 0.0,0.0
    mod=10
    
    ##<<COMPLETE>>
    for line in stream:
        record = line.split(',')
        uid = record[sample_col]
        # check if hash of uid falls in our randomly chosen bucket
        if(mmh3.hash(uid)%buckets==random_bucket):
            n=n+1
            transaction = float(record[3])

            sum1=sum1+transaction
            sqsum1 = sqsum1 + pow(transaction,2)
            
            if(n%mod==0):
                sum2=sum2+sum1/mod
                sqsum2=sqsum2+sqsum1/mod
                sum1=0
                sqsum1=0
            if(n%(mod*10)==0):
                sum2=sum2/10
                sqsum2=sqsum2/10
                mod=mod*10
                sum1=0
                sqsum1=0
        ##<<COMPLETE>>
        pass
    ##<<COMPLETE>>

    mean,standard_deviation = calculate(n,mod,sum1,sum2,sqsum1,sqsum2)

    #print(n)
    #if n > 0:
    #    mean = sum/n
    #    standard_deviation = pow(sqsum/n - mean*mean,1/2)
    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.C Timing

files=['transactions_small.csv', 'transactions_medium.csv','transactions_large.csv']
percents=[.02, .005]

if __name__ == "__main__": 

    ##<<COMPLETE: EDIT AND ADD TO IT>>
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            print("\nFile: ", f)
            fstream = open(f, "r")
            tic = time.clock()
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            toc = time.clock()
            print("Time taken: ",(toc - tic)*1000, " ms")
            fstream.close()
            fstream = open(f, "r")
            tic = time.clock()
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            toc = time.clock()
            print("Time taken: ",(toc - tic)*1000, " ms")

