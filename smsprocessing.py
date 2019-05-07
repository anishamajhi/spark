
from pyspark.sql import SparkSession 
from pyspark.sql.functions import explode 
from pyspark.sql.functions import split 
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,MapType,DoubleType,IntegerType
import re,subprocess
import time,json
import pandas as pd


def msgProcess(user,root,hdfs):
  spark = SparkSession.builder.appName("PythonSparkStreamingKafka_RM_01").getOrCreate()
  spark.conf.set('spark.executor.cores', '1')
  df = spark.read.option("header","true").csv(hdfs+"/"+root+"/"+user+"/Messages/raw/*")
  df.show()
  f = open("bankSendersMap1.json","r")
  se1 = f.read()
  se1 = se1.split("\n")
  se = {}
  for i in range(0,len(se1)-1):
        line = re.sub('[!@#$-]', '', se1[i].split(":")[0])
        se[line[2:].lower()] = se1[i].split(":")[1]

  def Sal(msg):
     if ('salary' in msg):
          return 1
     else:
         return 0

  Sal1 = udf(lambda z: Sal(z),IntegerType())

  def Currency(name):  
     pattern = re.compile("(Rs |RS |INR |Rs|RS|INR)")
     if(name != 0):
         result = pattern.match(name)
         if(result):
             name = name.replace(result.group(1),'')
             name = name.replace(',','')
             name = name.replace("?",'')
             if(len(name)>0):
              if(name[0] == "."):
                  return float(name[1:])
              else:
                 return float(name)
  
  Currency1 = udf(lambda z: Currency(z),DoubleType())

  def TagSender(send):
        line = re.sub('[!@#$-]', '', send)
        if(line[2:].lower() in se.keys()):
             if(se[line[2:].lower()] != "Others"):
               return str(se[line[2:].lower()])
        else:
            return str('Others')




  TagSender1 = udf(lambda z: TagSender(z),StringType())
  df = df.dropna(thresh=2)
  df.show()
  df = df.withColumn("SenderId", TagSender1(df.SenderId))
  with open("BankingRegex2.json") as f:
        regex = json.load(f)


        
  bank = {"DBS":0,"Allahabad":1,"Andhra":2,"HSBC":3,"Kotak":4,"Citi":5,"SBI":6,"HDFC":7,"ICICI":8,"Axis":9,"Federal":10,"IOB":11,"Standard Chartered":12,"Bank of Maharastra":13,"AMEX":14,"RBL":15,"PAYTM":16,"PHONEPE":17,"SODEXO":18,"IDBI":19,"PAYTM BANK":20}
  def dataExt(send,msg):
    out = {0:"a"}
    if(send in bank.keys()):
        bankMap = bank[send]
        List = regex[bankMap]['regexList']
        for j in range(len(List)):
             pattern = re.compile(List[j]['regex'])
             result = pattern.match(msg)
             if(result):
                  c = List[j]['attribute']
                  out['type'] = List[j]['type']
                  for k in range(len(c)):
                        out[c[k]] = result.group(k+1)
                  break
    return out




        
  dataExtUdf = udf(dataExt,MapType(IntegerType(),StringType()))
  x1 = df.withColumn("xyz", dataExtUdf(df.SenderId,df.MessageBody))
  x1.show()
  x2 = x1.toPandas()
  valu = [1,2,23,4,8,5,7,10,16,12,13,98,99,0]
  dfTag = pd.concat([x2,pd.DataFrame(columns = valu)],sort = False)
  dfTag = dfTag[dfTag.xyz != {0:'a'}]
  print(dfTag)
  if(len(dfTag)>0):
    for i in range(len(dfTag)):
      keys = dfTag.xyz.iloc[i].keys()
      for j in keys:
          if j in dfTag.columns:
              dfTag[j].iloc[i] = dfTag.xyz.iloc[i][j]

    dfTag = dfTag.fillna("0")
    dfTag = dfTag[['UserId', 'SenderId','MessageBody','SmsCreatedTime','CellId','Hash',1,2,4,5,7,8,10,12,13,16,23,98,99,0]]
    dfTag.columns = [ 'UserId','SenderId','MessageBody','SmsCreatedTime','CellId','Hash','AcctNo','CardNo','Txn_amt','Balance','DueDate','SpentAt','DueAmount','TxnType','OTP','MinDue','CardType','SpentType','refId','type']
    df1 = spark.createDataFrame(dfTag)
    t = str(int(time.time()))
    df1 = df1.withColumn("Txn_amt", Currency1(df1.Txn_amt))
    df1 = df1.withColumn("Is_Sal", Sal1(df1.MessageBody))
    df1.write.mode("append").option("header","true").csv(hdfs+"/"+root+"/"+user+"/Messages/output/"+t)
    df.write.mode("append").option("header","true").csv(hdfs+"/"+root+"/"+user+"/Messages/processed/"+t)
    raw_path = hdfs+"/"+root+"/"+user+"/Messages/raw/*"
    subprocess.call(["hadoop", "fs", "-rm", "-r", raw_path])
  else:
    t = str(int(time.time()))
    df.write.mode("append").option("header","true").csv(hdfs+"/"+root+"/"+user+"/Messages/processed/"+t)
    raw_path = hdfs+"/"+root+"/"+user+"/Messages/raw/*"
    subprocess.call(["hadoop", "fs", "-rm", "-r", raw_path])
