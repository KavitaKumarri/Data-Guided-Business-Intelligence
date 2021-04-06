import pandas as pd
import numpy as np
import sys
import random
import csv


       
def readData():
    
    with open('queries.txt') as queriesFile:
        queries = queriesFile.readlines()
    queries = [q.strip() for q in queries]
    
    budgetDicttemp = dict()
    bidsDict = dict()
    bidderData = pd.read_csv("bidder_dataset.csv")
    for i in range(0, len(bidderData)):
        advertiser = bidderData.iloc[i]['Advertiser']
        budget = bidderData.iloc[i]['Budget']
        bidValue = bidderData.iloc[i]['Bid Value']
        keyword = bidderData.iloc[i]['Keyword']
        
        if not advertiser in budgetDicttemp:
            budgetDicttemp[advertiser] = budget
        if not keyword in bidsDict:
            bidsDict[keyword]={}
        if not advertiser in bidsDict[keyword]:
            bidsDict[keyword][advertiser] = bidValue
            
    budgetDict = dict(budgetDicttemp)
    return [budgetDict,bidsDict,queries]



            
        
def getTopBidder_greedy(bids,budgetDict):
    bidsKeys = bids.keys()
    topBidder = bidsKeys[0]
    maxBidValue = -1
    exist = False
 
    for key in bidsKeys:
        if bids[key] <=budgetDict[key] :
            exist = True
            break
    if exist == False:
        return -1
    for key in bidsKeys:
        if bids[key] <= budgetDict[key]:
            if maxBidValue == bids[key] and topBidder > key:
                topBidder = key
                maxBidValue = bids[key]
            elif maxBidValue < bids[key]:
                maxBidValue = bids[key]
                topBidder = key
    
    return topBidder
    


def getTopBidder_balance(bids, budgetDict):
    bidsKeys = bids.keys()
    topBidder = bidsKeys[0]
    maxBidValue = -1
    exist = False
 
    for key in bidsKeys:
        if bids[key] <=budgetDict[key] :
            exist = True
            break
    if exist == False:
        return -1
    for key in bidsKeys:
        if bids[key] <= budgetDict[key]:
            if budgetDict[topBidder] == budgetDict[key] and topBidder > key:
                topBidder = key
            elif budgetDict[topBidder] < budgetDict[key]:
                topBidder = key
                
                
            
    return topBidder




def getTopBidder_MSVV(bids, budgetDict, remainingBudget):
    bidsKeys = bids.keys()
    topBidder = bidsKeys[0]
    maxBidValue = -1
    exist = False
 
    for key in bidsKeys:
        if bids[key] <=remainingBudget[key] :
            exist = True
            break
    if exist == False:
        return -1
    for key in bidsKeys:
        if bids[key] <= budgetDict[key]:
            x_key_top = (budgetDict[topBidder] - remainingBudget[topBidder])/budgetDict[topBidder]
            top_value = bids[topBidder]*(1-np.exp(x_key_top-1))
            x_key = (budgetDict[key] - remainingBudget[key])/budgetDict[key]
            key_value = bids[key]*(1-np.exp(x_key-1))
           
            if top_value == key_value and topBidder > key:
                topBidder = key
            elif top_value < key_value:
                topBidder = key
    return topBidder


def findRevenue(queries, budgetDict, bidsDict, algoInput):
    revenue = 0.0
    topBidder = -1
    
    remainingBudget = dict(budgetDict)
    for query in queries:
        if algoInput == "greedy":
            topBidder = getTopBidder_greedy(bidsDict[query],budgetDict)
        elif algoInput =="balance":
            topBidder = getTopBidder_balance(bidsDict[query],budgetDict)
        elif algoInput == "msvv":
            topBidder = getTopBidder_MSVV(bidsDict[query],budgetDict,remainingBudget)
        if topBidder>=0:
            revenue = revenue + bidsDict[query][topBidder]
            if algoInput == "greedy" or algoInput == "balance":
                budgetDict[topBidder] -= bidsDict[query][topBidder]
            elif algoInput == "msvv":
                remainingBudget[topBidder]-= bidsDict[query][topBidder]
                
    return revenue 



def findCompetitiveRatio(queries, budgetDict, bidsDict, algoInput):
    revenueSum = 0.0
    for i in range(0,100):
        random.shuffle(queries)
        temp = dict(budgetDict)
        revenue = findRevenue(queries, temp, bidsDict, algoInput)
        revenueSum+=revenue;
    rev = revenueSum/100
    return rev/sum(budgetDict.values())



#revenue = greedyAlgo(queries, budgetDict, bidsDict)
#print (revenue)

#revenue = balanceAlgo(queries, budgetDict, bidsDict)
#print (revenue)
#revenue = MSVVAlgo(queries, budgetDict, bidsDict, budgetDict)
#print(revenue)
def computeFinalOutput(algoInput):
    budgetDict,bidsDict, queries = readData()
    revenue = 0.0
    competitiveRatio = 0.0
    revenue = findRevenue(queries, budgetDict, bidsDict, algoInput)
    competitiveRatio = findCompetitiveRatio(queries, budgetDict, bidsDict, algoInput)
    print("revenue = " + str(revenue))
    print("competitive Ratio = " + str(competitiveRatio))
           
       

random.seed(0)
computeFinalOutput(sys.argv[1])

        
