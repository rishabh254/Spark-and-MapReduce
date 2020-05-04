## for SBU's Big Data Analytics Course 
## Homework 3 Part1
## Spring 2020
##
## Student Name: Rishabh Goel
## Student ID: 112714848

import sys
from pyspark import SparkContext
import json,re
import numpy as np
from scipy import stats

import pprint

def get_pVal(data_matrix,m):
    # finding beta values
    beta = data_matrix.map(lambda x : (x[0] , np.dot(np.linalg.inv(np.dot(np.transpose(x[1][:,:m]),x[1][:,:m])) ,np.dot(np.transpose(x[1][:,:m]),x[2]) ) , x[1] , x[2] ))

    # finding t value
    # Output format : (word,beta_1,df,tvalue)
    t = beta.map(lambda x : (x[0],x[1][1],(np.size(x[3])-np.size(x[2],1)), x[1][1]/np.sqrt((np.sum(np.square(x[3]-np.dot(x[2][:,:m],x[1]) )))/((np.size(x[3])-np.size(x[2],1))* np.sum(np.square(x[2][1]-np.mean(x[2][1]))) ) )) ) 

    # finding p value from t value
    p = t.map(lambda x : (x[0],x[1],stats.t.sf(np.abs(x[3]),x[2])*2))
    #p = t.map(lambda x : (x[0],x[1], 2*(1-scs.t.cdf(x[3], x[2])) ))

    # Output format : (word,beta_1,p_corrected_value)
    p_corrected = p.map(lambda x : (x[0],x[1],x[2]*1000))
    return p_corrected

# data standardization
def norm(x):
    return (x-np.mean(x))/np.std(x)

pp = pprint.PrettyPrinter(indent=4)
sc = SparkContext()
rdd = sc.textFile(sys.argv[1])

rdd_json = rdd.map(json.loads)
rdd_json = rdd_json.filter(lambda row : 'reviewText' in row)

# find 1000 most common valid words
common_words = sc.broadcast(rdd_json.flatMap(lambda line : re.findall(  r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))' ,line['reviewText'].lower()))\
               .map(lambda x : (x,1)).reduceByKey(lambda x,y : x+y)\
               .sortBy(lambda x : x[1],ascending=False)\
               .take(1000))

# reviews data per word
# Output format : (word,[(rel_freq,verified,overall),...])
observations_by_word = rdd_json.map(lambda row : (re.findall(  r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))' ,row['reviewText'].lower()),row['verified'],row['overall']) )\
           .filter(lambda row : len(row[0])!=0).flatMap(lambda row : [(word[0],[(row[0].count(word[0])/len(row[0]),row[1],row[2])]) for word in common_words.value]).groupByKey()\
           .map(lambda x : (x[0], list(x[1])))

# creating data matrix for multi linear regression
data_matrix = observations_by_word.map(lambda x : (x[0],[y[0][0] for y in x[1]],[y[0][2] for y in x[1]],[int(y[0][1]==True) for y in x[1]]))\
              .map(lambda x : (x[0],norm(x[1]),norm(x[2]), norm(x[3]) ))\
              .map(lambda x : (x[0], np.stack((np.ones(np.size(x[1])),x[1],x[3]), axis=-1),np.transpose(x[2])))
 
p1 = get_pVal(data_matrix,2)
p2 = get_pVal(data_matrix,3)

betaTop = p1.sortBy(lambda x : x[1],ascending=False)
betaBottom = p1.sortBy(lambda x : x[1],ascending=True)
print('The top 20 word positively correlated with rating')
pp.pprint(betaTop.take(20))
print('The top 20 word negatively correlated with rating')
pp.pprint(betaBottom.take(20))
print("\n")

betaTop = p2.sortBy(lambda x : x[1],ascending=False)
betaBottom = p2.sortBy(lambda x : x[1],ascending=True)
print('The top 20 words positively related to rating, controlling for verified')
pp.pprint(betaTop.take(20))
print('The top 20 words negatively related to rating, controlling for verified')
pp.pprint(betaBottom.take(20))




