import datetime
import urllib
import mysql.connector
import string
import re
import os



	
class Option:

	def __init__(self,inputSymbol):
		self.listOfOptionPages=[]
		self.listOfOptions=[]
		self.Symbol=inputSymbol
		return None
		
	def getAllOptionPages(self):
            print 'http://finance.yahoo.com/q/op?s='+self.Symbol
            dataPulled=urllib.urlopen('http://finance.yahoo.com/q/op?s='+self.Symbol).read()
            listOfOptionCodes=re.findall('<option data-selectbox-link="(.*?)" value=',dataPulled)
            for i in range(0,len(listOfOptionCodes)):
                    self.listOfOptionPages.append('http://finance.yahoo.com/'+listOfOptionCodes[i])
            return 0
	
	def readOptionsFromPage(self):
            now=str(datetime.datetime.now())
            for j in range(0, len(self.listOfOptionPages)):
               #from each Option Page pull all of the Puts and Calls
               print len(self.listOfOptionPages)
               dataPulled=urllib.urlopen(self.listOfOptionPages[j]).read()
               dataPulled=re.sub('\s','',dataPulled)
               AsOf=re.findall('lstTrdTime"\>(.*?)\<',dataPulled)
               optionRows=re.findall('\<trdata(.*?)tr\>',dataPulled)
               #print len(optionRows)
               for i in range(0,len(optionRows)):
                  optionRows[i]=re.sub('\>\<','XXXXXX',optionRows[i])
                  oneRow=re.findall('\>(.*?)\<',optionRows[i])
                  oneRow.append(AsOf[0])
                  oneRow.append(now)
                  oneRow.append(self.Symbol)
                  
                  self.listOfOptions.append(oneRow)
                  print oneRow
            return 0

        def sendToDB(self,connectionToDB):
            
            nameDB=re.sub('\^','',self.Symbol)
            addDatabase=("CREATE DATABASE if not exists "+nameDB)
            selectDatabase=("USE "+nameDB)

            cursor=connectionToDB.cursor()
            cursor.execute(addDatabase)
            cursor.execute(selectDatabase)
            
            for i in range(0,len(self.listOfOptions)):
                updateTable="INSERT INTO %s (Strike,ContractName,Last,Bid,Ask,AmountChange,PercentChange,Volume,OpenIntrest,ImpliedVolatility,LastTrade,DateTime,stockSymbol) VALUES ( %s )"  % (self.listOfOptions[i][1],re.sub('[\[|\]|\^|\%]','', str(self.listOfOptions[i]) ) )
                addTable='CREATE TABLE if not exists %s (timeID INT NOT NULL PRIMARY KEY AUTO_INCREMENT, Strike DOUBLE,ContractName CHAR(25),Last DOUBLE,Bid DOUBLE, Ask DOUBLE, AmountChange DOUBLE, PercentChange DOUBLE, Volume DOUBLE, OpenIntrest DOUBLE, ImpliedVolatility DOUBLE, LastTrade Char(12), DateTime DATETIME, stockSymbol CHAR(10))' % (self.listOfOptions[i][1])
                cursor.execute(addTable)
                cursor.execute(updateTable)
            
            connectionToDB.commit()
            cursor.close()
            return 0
		
	def printOptionPages(self):
                print len(self.listOfOptionPages)
		for i in range(0,len(self.listOfOptionPages)):
			print self.listOfOptionPages[i]
		return 0
	
        def printOptions(self):
            for i in range(0,len(self.listOfOptions)):
                print self.listOfOptions[i]
            return 0
        
        
                
				


def getSymbols(inputText):
    listOfStocks=[]
    for	line in	open(inputText):
		newline=re.sub('\s','',line)
		listOfStocks.append(str(newline))
		print newline

    return listOfStocks


os.chdir('/home/teddy/Production/stocks')
#hst='192.168.1.5'
hst='localhost'
un='username'
pw='password'
fileOfSymbols='symbols.csv'

myTime=datetime.datetime.now()
CurrentMinutes=myTime.hour*60+myTime.minute


#CurrentMinutes=880
if 9*60+30<=CurrentMinutes<=16*60+30:
    cnx=mysql.connector.connect(user=un, password=pw)
    listOfStocks=getSymbols(fileOfSymbols)
    listOfRetrievedStocks=[]
    for i in range(0,len(listOfStocks)):
        OptionObject=Option(listOfStocks[i])
        OptionObject.getAllOptionPages()
        OptionObject.readOptionsFromPage()
	listOfRetrievedStocks.append(OptionObject)
        #OptionObject.printOptions()
        #OptionObject.sendToDB(cnx)
    for i in range(0,len(listOfRetrievedStocks)):
	b=listOfRetrievedStocks.pop()
	b.sendToDB(cnx)
    
    cnx.close()       
else:
    print "Outside of Business	hours"
    print myTime.hour*60+myTime.minute

