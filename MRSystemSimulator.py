##########################################################################
## MRSystemSimulator2020.py  v 0.1
##
## Implements a basic version of MapReduce intended to run
## on multiple threads of a single system. This implementation
## is simply intended as an instructional tool for students
## to better understand what a MapReduce system is doing
## in the backend in order to better understand how to
## program effective mappers and reducers. 
##
## MyMapReduce is meant to be inheritted by programs
## using it. See the example "WordCountMR" class for 
## an exaample of how a map reduce programmer would
## use the MyMapReduce system by simply defining
## a map and a reduce method. 
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
from scipy import sparse
import mmh3
from random import random


##IO, Process Imports: 
import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint


##########################################################################
##########################################################################
# MapReduceSystem: 

class MapReduce:
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3, use_combiner = False): 
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks
        self.use_combiner = use_combiner #whether or not to use a combiner within map task
        
    ###########################################################   
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): 
        print("Need to override map")

    
    @abstractmethod
    def reduce(self, k, vs): 
        print("Need to overrirde reduce")
        

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r, combiner=False):
        #runs the mappers on each record within the data_chunk and assigns each k,v to a reduce task
        mapped_kvs = [] #stored keys and values resulting from a map 
        for (k, v) in data_chunk:
            #run mappers:
            chunk_kvs = self.map(k, v) #the resulting keys and values after running the map task
            mapped_kvs.extend(chunk_kvs) 

	#assign each kv pair to a reducer task
        if combiner:
            #do the reduce from here before passing to reduceTask

            #<<COMPLETE>>
            #pass
            #1. Setup value lists for reducers
            #<<COMPLETE>>            

            #2. call reduce, appending result to get passed to reduceTasks
            #<<COMPLETE>>
            vsPerK = dict()
            for (k, v) in mapped_kvs:
                try:
                    if type(v) is tuple:
                        vsPerK[k] = tuple(map(sum, zip(v,vsPerK[k])))
                    else:
                        vsPerK[k] += v
                except KeyError:
                    vsPerK[k] = v

            #call reducers on each key with a list of values
            #and append the result for each key to namenoe_fromR
            for (k, vs) in vsPerK.items():
                namenode_m2r.append((self.partitionFunction(k), (k, vs)))
                
        else:
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))


    def partitionFunction(self,k): 
        #given a key returns the reduce task to send it
        ##<<COMPLETE>>
        node_number = mmh3.hash(str(k))%self.num_reduce_tasks
        return node_number


    def reduceTask(self, kvs, namenode_fromR): 
        #sort all values for each key (can use a list of dictionary)
        vsPerK = dict()
        for (k, v) in kvs:
            try:
                vsPerK[k].append(v)
            except KeyError:
                vsPerK[k] = [v]


        #call reducers on each key with a list of values
        #and append the result for each key to namenoe_fromR
        for k, vs in vsPerK.items():
            if vs:
                fromR = self.reduce(k, vs)
                if fromR:#skip if reducer returns nothing (no data to pass along)
                    namenode_fromR.append(fromR)

		
    def runSystem(self): 
        #runs the full map-reduce system processes on mrObject

	#the following two lists are shared by all processes
        #in order to simulate the communication
        namenode_m2r = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
                                          #in the form [(k, v), ...]

	#Divide up the data into chunks according to num_map_tasks
        #Launch a new process for each map task, passing the chunk of data to it. 
        #Hint: The following starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()  
        runningProcesses = []
        ## <<COMPLETE>>


        # divide data into approximately equal sized chunks to asssign to the mappers
        chunkSize = int(round(len(self.data)/self.num_map_tasks))
        for i in range(self.num_map_tasks):
            start = i*chunkSize
            end = (i+1)*chunkSize
            if i == self.num_map_tasks-1:
                end = len(self.data)
            p = Process(target=self.mapTask, args=(data[start:end],namenode_m2r,self.use_combiner))
            p.start()
            runningProcesses.append(p)
        
        
	#join map task running processes back
        for p in runningProcesses:
            p.join()
		        #print output from map tasks 
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

	#"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)] 
        ## <<COMPLETE>>
        for tup in namenode_m2r:
            to_reduce_task[tup[0]].append(tup[1])
        
        #launch the reduce tasks as a new process for each. 
        runningProcesses = []
        for kvs in to_reduce_task:
            runningProcesses.append(Process(target=self.reduceTask, args=(kvs, namenode_fromR)))
            runningProcesses[-1].start()

        #join the reduce tasks back
        for p in runningProcesses:
            p.join()
        #print output from reducer tasks 
        print("namenode_fromR after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        #return all key-value pairs:
        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:
            
class WordCountBasicMR(MapReduce): #[DONE]
    #mapper and reducer for a more basic word count 
	# -- uses a mapper that does not do any counting itself
    def map(self, k, v):
        kvs = []
        counts = dict()
        for w in v.split():
            kvs.append((w.lower(), 1))
        return kvs

    def reduce(self, k, vs): 
        return (k, np.sum(vs))  

#an example of another map reducer
class SetDifferenceMR(MapReduce): 
    #contains the map and reduce function for set difference
    #Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, k, v):
        toReturn = []
        for i in v:
            toReturn.append((i, k))
        return toReturn

    def reduce(self, k, vs):
        if len(vs) == 1 and vs[0] == 'R':
            return k
        else:
            return None

class MeanCharsMR(MapReduce): #[TODO]
    def map(self, k, v):
        #<<COMPLETE>>
        kvs=[]
        # initialize char dict
        pairs = dict()
        for i in range(0,26):
            pairs[chr(97+i)]=0

        # calculate count of each char in the document
        for w in v.split():
            for c in w.lower():
                try:
                    pairs[c] = pairs[c]+1
                except KeyError:
                    pass 

        # kvs format : (char_key, (sum_of_char, sum_of_squares_of_char, no_docs))
        for i in range(0,26):
            kvs.append((chr(97+i),(pairs[chr(97+i)],pow(pairs[chr(97+i)],2),1)))
        return kvs
        
    
    def reduce(self, k, vs):
        S = sum([pair[0] for pair in vs])
        SS = sum([pair[1] for pair in vs])
        N = sum([pair[2] for pair in vs])
        mean = S/N
        std = pow(SS/N - mean*mean,1/2)
        #<<COMPLETE>>
        # return (char_key, (sum_of_char, sum_of_squares_of_char, no_docs, mean_char, std_char))
        return (k , (S, SS, N, round(mean, 2),round(std, 2)))

			
			
##########################################################################
##########################################################################

from scipy import sparse
def createSparseMatrix(X, label):
	sparseX = sparse.coo_matrix(X)
	list = []
	for i,j,v in zip(sparseX.row, sparseX.col, sparseX.data):
		list.append(((label, i, j), v))
	return list

if __name__ == "__main__": #[Uncomment peices to test]
    
    ###################
    ##run WordCount:
    
    print("\n\n*****************\n Word Count\n*****************\n")
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful")]
    print("\nWord Count Basic WITHOUT Combiner:")
    mrObjectNoCombiner = WordCountBasicMR(data, 3, 3)
    mrObjectNoCombiner.runSystem()
    print("\nWord Count Basic WITH Combiner:")
    mrObjectWCombiner = WordCountBasicMR(data, 3, 3, use_combiner=True)
    mrObjectWCombiner.runSystem()
    

    ###################
    ##MeanChars:
    print("\n\n*****************\n Word Count\n*****************\n")
    data.extend([(8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
                 (9, "The car raced past the finish line just in time."),
	         (10, "Car engines purred and the tires burned.")])
    print("\nMean Chars WITHOUT Combiner:")
    mrObjectNoCombiner = MeanCharsMR(data, 4, 3)
    mrObjectNoCombiner.runSystem()
    print("\nMean Chars WITH Combiner:")
    mrObjectWCombiner = MeanCharsMR(data, 4, 3, use_combiner=True)
    mrObjectWCombiner.runSystem()
      


	
