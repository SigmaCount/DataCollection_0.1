import csv
import datetime
import urllib
import mysql.connector
import string
import os
import re



def getSymbols(inputText):
    listOfStocks=[]
    for	line in	open(inputText):
		newline=re.sub('\s','',line)
		listOfStocks.append(str(newline))
		#print newline
    return listOfStocks

def getVariables(fileOfVariables):
    name=[]
    code=[]
    dtype=[]
    for line in open(fileOfVariables):
        newline=re.sub('\s','',line)
       # print newline
        namecode=newline.split(',')
        name.append(namecode[0])
        code.append(namecode[1])
        dtype.append(namecode[0]+' '+namecode[2])
    
    stringFields=str(name)
    stringFields=re.sub('[\[|\]|\'|\s]','',stringFields)
    pullCode=str(code)
    pullCode=re.sub('[\[|\]|\'|\s|,]','',pullCode)
    dataType=str(dtype)
    dataType=re.sub('[\[|\]|\']','',dataType)
    return (stringFields, pullCode, dataType)


class Stock:
    def __init__(self,Symbol,stringFields,pullCode,dataType):
        self.now=str(datetime.datetime.now())
        url='http://finance.yahoo.com/d/quotes.csv?s=%s&f=%s' % (Symbol, pullCode)
        #awData=urllib.urlopen(url).read().split(',')
        #print awData
        print url
        
        self.rawData=urllib.urlopen(url).read()
        print self.rawData
        self.rawData=re.sub('N/A','NULL',self.rawData)
        self.rawData=re.sub('[\"|\s]','',self.rawData)
        self.rawData=re.sub(',','\',\'',self.rawData)
        self.rawData="'%s','%s'" % (self.rawData, self.now)
        
        
        self.stringFields=stringFields
        self.dataType=dataType
        self.Symbol=re.sub('\^','',Symbol)
        return None
     
    def printData(self):
        stringFieldsX=self.stringFields.split(',')
        dataTypeX=self.dataType.split(',')
        rawDataX=self.rawData.split(',')
        for i in range(0,len(stringFieldsX)):
            print '%s, %s, %s' %(stringFieldsX[i],dataTypeX[i],rawDataX[i])
        return 0
    
    
    def sendToDB(self,connectionToDB):
        addDatabase = 'CREATE DATABASE if not exists %s' % (self.Symbol)
        selectDatabase="USE %s " % (self.Symbol)
        addTable='CREATE TABLE if not exists %s (timeID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,%s,DateTime DATETIME)' % (self.Symbol, self.dataType)
        updateTable='INSERT INTO %s ( %s ,DateTime) VALUES (%s)' % (self.Symbol, self.stringFields, self.rawData)
        
        #print self.rawData
        #print updateTable
        cursor=connectionToDB.cursor()
        cursor.execute(addDatabase)
        cursor.execute(selectDatabase)
        cursor.execute(addTable)
        cursor.execute(updateTable)
        connectionToDB.commit()
        cursor.close()
        return 0
    


os.chdir('/home/teddy/Production/stocks')
#hst='192.168.1.5.'
un='username'
pw='password'
symbolscsv='symbols.csv'
variablecsv='yahoolist3.csv'



myTime=datetime.datetime.now()
CurrentMinutes=myTime.hour*60+myTime.minute


#CurrentMinutes=880
if 9*60+30<=CurrentMinutes<=16*60+30:
    print 'Nothing'
    listOfStocks=getSymbols(symbolscsv)
    listOfRetrievedStocks=[]
    print listOfStocks
    stringFields, pullCode, dataType = getVariables(variablecsv)
    print stringFields
    
    cnx=mysql.connector.connect(user=un, password=pw)
    for i in range(0,len(listOfStocks)):
        print listOfStocks[i]
        a=Stock(listOfStocks[i], stringFields, pullCode,dataType)
        listOfRetrievedStocks.append(a)
	#a.printData()
        #a.sendToDB(cnx)
    for i in range(0,len(listOfRetrievedStocks)):
	b=listOfRetrievedStocks.pop()
	b.sendToDB(cnx)
    cnx.close() 
else:
    print "Outside of Business	hours"
    print myTime.hour*60+myTime.minute


