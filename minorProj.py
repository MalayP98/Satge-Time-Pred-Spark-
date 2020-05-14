import numpy as np
import pandas as pd
from numpy import random as rd
from pyspark import SparkContext, SparkConf
from odf import text, teletype
from odf.opendocument import load

# spark://master:7077
class init_spark:
    def __init__(self, cores, appName, log_level):
        conf = SparkConf().setMaster("local[{}]".format(cores)).setAppName(appName)
        self.sc = SparkContext(conf=conf)
        self.sc.setLogLevel(log_level)


class Dataset:
    def __init__(self, jobs, sprkCtx, set_size):
        self.__jobs = jobs
        self.__sprkCtx = sprkCtx
        self.__set_size = set_size
        self.__trans_key = ["filter", "map", "distinct", "union", "intersection",
                            "coalesce", "join", "tupleSize"]
        self.init_rdd = None
        self.stageTime = []

    def genrate_dataset(self):
        dataset = []
        for i in range(self.__jobs):
            trans = rd.random_integers(0, 1, len(self.__trans_key) - 1)
            tup_size = rd.random_integers(2, 6)
            job = np.append(trans, tup_size)
            dataset.append(list(job))

        for i in range(self.__jobs):
            print("*" * 20 + "Job {}".format(i + 1) + "*" * 20)
            self.start_job(dataset[i])

        return dataset

    @staticmethod
    def filter_f1(x):
        ele = rd.random_integers(1, 10000)
        index = rd.random_integers(0, x)
        return ele, index

    @staticmethod
    def map_f1():
        # arr = np.array(x)
        # tup = tuple(arr+1)
        # return tup
        operator = rd.random_integers(1, 4)
        operand = rd.random_integers(1, 10000)
        return operator, operand
        # arr = np.array(x)
        # if operator == 1:
        #     tup = tuple(arr+operand)
        #     return tup
        # elif operator == 2:
        #     tup = tuple(arr-operand)
        #     return tup
        # elif operator == 3:
        #     tup = tuple(arr*(operand/10))
        #     return tup
        # elif operator == 4:
        #     tup = tuple(arr/(operand/10))
        #     return tup

    def show_dataset(self):
        dataset = self.genrate_dataset()
        df = pd.DataFrame(dataset, columns=self.__trans_key)
        return df

    def start_job(self, job):
        # print("job is", job)
        data = []
        RDDList = []
        tup_size = job[-1]
        for i in range(self.__set_size):
            tup = tuple(rd.random_integers(1, 1000000, tup_size))
            data.append(tup)

        init_rdd = self.__sprkCtx.parallelize(data)
        self.init_rdd = init_rdd
        RDDList.append(init_rdd)
        for i in range(len(job) - 1):
            # print(job[i])
            if i == 0 and job[i] == 1:
                ele, index = self.filter_f1(tup_size - 1)
                # print("x[{}] > {}".format(index, ele))
                newRDD = RDDList[-1].filter(lambda x: x[index] > ele)
                # print(newRDD.first())
                RDDList.append(newRDD)
            elif i == 1 and job[i] == 1:
                # print("map")
                operator, opnd = self.map_f1()
                if operator == 1:
                    newRDD = RDDList[-1].map(lambda x: tuple(np.array(x) + opnd))
                elif operator == 2:
                    newRDD = RDDList[-1].map(lambda x: tuple(np.array(x) - opnd))
                elif operator == 3:
                    newRDD = RDDList[-1].map(lambda x: tuple(np.array(x) / opnd))
                elif operator == 4:
                    newRDD = RDDList[-1].map(lambda x: tuple(np.array(x) * (opnd) / 10))
                RDDList.append(newRDD)
                # print(newRDD.first())
            elif i == 2 and job[i] == 1:
                newRDD = RDDList[-1].distinct()
                RDDList.append(newRDD)
                # print(newRDD.first())
            elif i == 3 and job[i] == 1:
                rndNum = rd.random_integers(0, len(RDDList) - 1)
                # print("union at {}".format(rndNum))
                newRDD = RDDList[-1].union(RDDList[rndNum])
                RDDList.append(newRDD)
                # print(newRDD.first())
            elif i == 4 and job[i] == 1:
                rndNum = rd.random_integers(0, len(RDDList) - 1)
                # print("intersection with {}".format(rndNum))
                newRDD = RDDList[-1].intersection(RDDList[rndNum])
                if newRDD.isEmpty(): newRDD = RDDList[-1]
                RDDList.append(newRDD)
                # print(newRDD.first())
            elif i == 5 and job[i] == 1:
                rndNum = rd.random_integers(1, 5)
                # print("coalesce at {}".format(rndNum))
                newRDD = RDDList[-1].coalesce(int(rndNum))
                RDDList.append(newRDD)
                # print(newRDD.first())
            elif i == 6 and job[i] == 1:
                rndNum = rd.random_integers(0, len(RDDList) - 1)
                # print("join at {}".format(rndNum))
                newRDD = RDDList[-1].join(RDDList[rndNum])
                if newRDD.isEmpty(): newRDD = RDDList[-1].join(RDDList[-1])
                RDDList.append(newRDD)
                # print(newRDD.first())
        RDDList[-1].first()

    @staticmethod
    def extract_time(log):
        space = -1
        for i in range(len(log)-1, -1, -1):
            if log[i] == " ": space += 1
            if space == 0: end_index = i-1
            if space == 1: start_index = i-1; break
        return log[start_index:end_index]

    def getStageTime(self):
        log_file = load("/home/malay/Desktop/Log.odt")
        allparas = log_file.getElementsByType(text.P)
        job_id = -1
        for i in range(len(allparas)):
            nextLog = None
            str = teletype.extractText(allparas[i])
            if str[0] == "*": job_id += 1
            if i + 2 < len(allparas):
                nextLog = teletype.extractText(allparas[i + 2])
            if nextLog != None and nextLog[0] == "*":
                print("time for job {} -> {}".format(job_id, str))
                print(self.extract_time(str))
            if nextLog == None:
                print("time for job {} -> {}".format(job_id, str))
                print(self.extract_time(str))
                break

    @staticmethod
    def extract_time(log):
        space = -1
        for i in range(len(log) - 1, -1, -1):
            if log[i] == " ":
                space += 1
                if space == 0: end_index = i
                if space == 1: start_index = i + 1; break
        return log[start_index:end_index]

    def getStageTime(self):
        log_file = load("/home/malay/Desktop/Log.odt")
        allparas = log_file.getElementsByType(text.P)
        job_id = -1
        for i in range(len(allparas)):
            nextLog = None
            str = teletype.extractText(allparas[i])
            if str[0] == "*": job_id += 1
            if i + 2 < len(allparas):
                nextLog = teletype.extractText(allparas[i + 2])
            if nextLog != None and nextLog[0] == "*":
                print("time for job {} -> {}".format(job_id, str))
                self.stageTime.append(int(self.extract_time(str))*1000)
            if nextLog == None:
                print("time for job {} -> {}".format(job_id, str))
                self.stageTime.append(int(self.extract_time(str)) * 1000)
                break


spark = init_spark(4, "Predict Job Time", "INFO").sc

dataset = Dataset(100, spark, 100)
dataset.genrate_dataset()



