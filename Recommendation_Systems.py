## for SBU's Big Data Analytics Course 
## Homework 3 Part2
## Spring 2020
##
## Student Name: Rishabh Goel
## Student ID: 112714848

from pyspark import SparkContext
import json,re
import numpy as np
import pprint
import sys

#rdd = sc.textFile('hdfs:/data/Software_5.json.gz')

#initializations
pp = pprint.PrettyPrinter(indent=4)
sc = SparkContext()

#reading rdd and converting to json
rdd = sc.textFile(sys.argv[1])
input_items = eval(sys.argv[2])

#filtering to only one rating per user per item to get most recent rating
#Outputs ((reviewerID,asin),overall)
user_item_ratings = rdd.map(json.loads)\
                    .sortBy(lambda review : review['unixReviewTime'],ascending=False)\
                    .map(lambda review : ((review['reviewerID'],review['asin']),review['overall'])).groupByKey().map(lambda x : (x[0], list(x[1])[0] ))

#filtering to items associated with at least 25 distinct users
item_a = sc.broadcast(user_item_ratings.map(lambda x : (x[0][1],1)).reduceByKey(lambda x,y: x+y).filter(lambda x : x[1]>=25).map(lambda x : x[0]).collect())
user_item_ratings = user_item_ratings.filter(lambda x : x[0][1] in item_a.value)

#filtering to users associated with at least 5 distinct items
user_a = sc.broadcast(user_item_ratings.map(lambda x : (x[0][0],1)).reduceByKey(lambda x,y: x+y).filter(lambda x : x[1]>=5).map(lambda x : x[0]).collect())
user_item_ratings = user_item_ratings.filter(lambda x : x[0][0] in user_a.value)


# represents the rows of utility matrix
# Output format : (asin,[(reviewerID,overall),...],mean)
ratings_by_item = user_item_ratings.map(lambda x : (x[0][1], (x[0][0],x[1])) ).groupByKey().map(lambda x : (x[0], list(x[1]), sum([a[1] for a in list(x[1])])/len(list(x[1])) ))

# filtering the items we need the predictions for
user_rating_present = ratings_by_item.filter(lambda x : x[0] in input_items)

# finding neighbors with the intersection of users_with_ratings for the two is > 1
# Output format : ((asin1,asin2),[(overall1,overall2),...],(mean1,mean2))
item_neighbors = user_rating_present.cartesian(ratings_by_item)\
                 .map(lambda x :((x[0][0],x[1][0]),[(dict(x[0][1])[k],dict(x[1][1])[k] ) for k in dict(x[0][1]).keys()&dict(x[1][1]).keys()], (x[0][2],x[1][2])  ) )\
                 .filter(lambda x : len(x[1])>1)

# finding similarity between items using pearson correlation and keeping neighbors with only possitive similarity
# Output format : ((asin1,asin2),sim(asin1,asin2))
item_similarity = item_neighbors.map(lambda x : (x[0],round(np.sum(np.product(x[1]-np.array(x[2]),1))/np.product(np.sqrt(np.sum(np.square(x[1]-np.array(x[2])),0))),2)))\
                  .filter(lambda x : x[0][0]!=x[0][1] and False==np.isnan(x[1]) and x[1]>0)

# finding the top 50 neighbors
# Output format : (asin,[(nbr1,sim(asin,nbr1)),...])
item_top_neighbors = item_similarity.map(lambda x : ((x[0][0]),(x[0][1],x[1]) ))\
                     .groupByKey().map(lambda x : (x[0], list(x[1])))\
                     .map(lambda x : (x[0],sorted(x[1],key = lambda y: y[1], reverse=True)[:50]))

# represents the cols of utility matrix
# Output format : (reviewerId,[(asin,overall),...])
ratings_by_user = user_item_ratings.map(lambda x : (x[0][0], (x[0][1],x[1])) ).groupByKey().map(lambda x : (x[0], list(x[1])))

# finding the rating and similarity value for asin neighbors per user
# Output format : ((reviewerID,asin),[(overall1,sim1),...])
users_to_predict = ratings_by_user.cartesian(item_top_neighbors)\
                   .map(lambda x :((x[0][0],x[1][0]),[(dict(x[0][1])[k],dict(x[1][1])[k] ) for k in dict(x[0][1]).keys()&dict(x[1][1]).keys()]  ) )\
                   .filter(lambda x : len(x[1])>1)

# all ratings (already present+predicted) for users for given asin
# Output format : ((asin),[(reviewerID1,pred_overall1),...])
user_rating_all = users_to_predict.map(lambda x : (x[0],round(np.sum(np.product(x[1],1))/np.sum(x[1],0)[1],2)))\
                  .map(lambda x : (x[0][1], (x[0][0],x[1])))\
                  .groupByKey().map(lambda x : (x[0], list(x[1])))

# predicted ratings for users for given asin
# Output format : ((asin),[(reviewerID1,pred_overall1),...])
user_rating_prediction = user_rating_all.join(user_rating_present)\
                         .map(lambda x : (x[0],  [(k ,dict(x[1][0])[k]) for k in set(dict(x[1][0])) - set(dict(x[1][1]))]   ))

#output = user_rating_present.join(user_rating_prediction).map(lambda x :  (x[0],x[1][0]+x[1][1])).collect()
output = user_rating_prediction.collect()
pp.pprint(output)
