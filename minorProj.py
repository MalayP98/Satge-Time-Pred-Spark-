import numpy as np
import pandas as pd
from numpy import random as rd
from pyspark import SparkContext, SparkConf

#spark://master:7077
class init_spark:
    def __init__(self, cores, appName, log_level):
        conf = SparkConf().setMaster("local[{}]".format(cores)).setAppName(appName)
        self.sc = SparkContext(conf=conf)
        self.sc.setLogLevel(log_level)

class Dataset:
    def __init__(self, jobs, sprkCtx, set_size):
        self.jobs = jobs
        self.__sprkCtx = sprkCtx
        self.set_size = set_size
        self.__trans_key = ["filter", "map", "flatMap", "mapPartioning", "union",
                          "intersection", "join", "coalase", "tupleSize"]

    def genrate_dataset(self):
        dataset = []
        for i in range(self.jobs):
            trans = rd.random_integers(0, 1, 8)
            tup_size = rd.random_integers(2, 6)
            job = np.append(trans, tup_size)
            dataset.append(list(job))

        for i in range(self.jobs):
            print("*"*20 + "Job {}".format(i+1) + "*"*20)
            self.start_job(dataset[i])

        return dataset

    def filter_f1(self, x):
        oprator = rd.random_integers(1, 2)
        ele = rd.random_integers(1, 10000)
        index = rd.random_integers(1, len(x))
        if oprator == 1: return x[index] > ele
        elif oprator == 2: return x[index] < ele

    def map_f1(self, x):
        operator = rd.random_integers(1, 4)
        operand = rd.random_integers(1, 10000)
        arr = np.array(x)
        if operator == 1:
            tup = tuple(arr+operand)
            return tup
        elif operator == 2:
            tup = tuple(arr-operand)
            return tup
        elif operator == 3:
            tup = tuple(arr*(operand/10))
            return tup
        elif operator == 4:
            tup = tuple(arr/(operand/10))
            return tup

    def show_dataset(self):
        dataset = self.genrate_dataset()
        df = pd.DataFrame(dataset, columns=self.__trans_key)
        return df

    def start_job(self, job):
        data = []
        RDDList = []
        tup_size = job[-1]
        for i in range(self.set_size):
            tup = tuple(rd.random_integers(1, 1000000, tup_size))
            data.append(tup)

        init_rdd = self.__sprkCtx.parallelize(data)
        RDDList.append(init_rdd)
        for i in range(1, len(job)):
            if i-1 == 0 and job[i-1] == 1:
                newRDD = RDDList[i-1].filter(self.filter_f1)
            elif i-1 == 1 and job[i-1] == 1:
                newRDD = RDDList[i - 1].map(self.map_f1)
            elif i - 1 == 1 and job[i - 1] == 1:
                newRDD = RDDList[i-1].flatMap()
            elif i - 1 == 1 and job[i - 1] == 1:pass
            elif i - 1 == 1 and job[i - 1] == 1:pass
            elif i - 1 == 1 and job[i - 1] == 1:pass
            elif i - 1 == 1 and job[i - 1] == 1:pass
            elif i - 1 == 1 and job[i - 1] == 1:pass




spark = init_spark(4, "Predict Job Time", "INFO").sc

dataset = Dataset(20, spark, 100)
df = dataset.show_dataset()


