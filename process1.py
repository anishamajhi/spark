from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import json,string,random,sys,time
from pyspark.sql import SparkSession
from time import sleep
import AddDeviceInfoRequest,Sms,Contact,CallLog,Location,BatteryStat,AppStat
import pandas as pd
import configparser
import sys
import smsprocessing

#conf = SparkConf().setAppName("PythonSparkStreamingKafka_RM_01").set("spark.shuffle.service.enabled", "false").set("spark.dynamicAllocation.enabled", "false").set("spark.io.compression.codec", "snappy").set("spark.rdd.compress", "true").set("spark.cores.max", "1")
#sc = SparkContext(conf=conf)

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")
sc.setLocalProperty("spark.scheduler.pool", "pool")
spark = SparkSession.builder.appName("PythonSparkStreamingKafka_RM_01").getOrCreate()
spark.conf.set('spark.executor.cores', '3')
sc.getConf().getAll()
ssc = StreamingContext(sc, 1)
config = configparser.ConfigParser()
config.read('config.ini')
x = sys.argv[1]
topic = sys.argv[2]
if('development' in (x)):
      env=0
elif('staging' in (x)):
      env=1
else:
      env=2

if(env==1):
     arg='STAGING'
elif(env==0):
     arg='DEVELOPMENT'
elif(env==2):
     arg='PRODUCTION'

hdfs = config[arg]['hdfs']
root = config[arg]['root'] + "/Users"
if(topic == 'sms'):
   kvs = KafkaUtils.createDirectStream(ssc, [config[arg]['SMS']], {"metadata.broker.list": config[arg]['kafka_broker']},keyDecoder = lambda x: x,valueDecoder = lambda x: x)
   dstream =kvs.map(lambda x: (x[1]))
   s = []
   def process(rdd):
     print("rdddd",rdd.collect())
     #appended = spark.createDataFrame([("","","","","","","")],["UserId","SenderId","MessageBody","SmsCreatedTime","Hash","CellId","DeviceId"])
     for i in rdd.collect():
       sid = Sms.Sms.GetRootAsSms(i, 0).SenderId()
       msg = Sms.Sms.GetRootAsSms(i, 0).MessageBody()
       sct = Sms.Sms.GetRootAsSms(i, 0).SmsCreatedTime()
       hash = Sms.Sms.GetRootAsSms(i, 0).Hash()
       cid = Sms.Sms.GetRootAsSms(i, 0).CellId()
       did = Sms.Sms.GetRootAsSms(i, 0).DeviceId()
       user = Sms.Sms.GetRootAsSms(i, 0).UserId()
       if(len(user)>0):
         #newRow = spark.createDataFrame([(user.decode("utf-8"),sid.decode("utf-8"),msg.decode("utf-8"),sct,hash.decode("utf-8"),cid.decode("utf-8"),did.decode("utf-8"))])
         #appended = appended.union(newRow)
         #appended.show()
         print(msg,"msgjkfvjkshdb")
         ms = msg.decode()
         s.append((user.decode("utf-8"),sid.decode("utf-8"),ms,sct,hash.decode("utf-8"),cid.decode("utf-8"),did.decode("utf-8"))) 
     if(len(rdd.collect())>0):
       appended = spark.createDataFrame(s,["UserId","SenderId","MessageBody","SmsCreatedTime","Hash","CellId","DeviceId"])
       t = "/"+str(int(time.time()))
       appended.write.mode('append').option("header", "true").csv(hdfs+"/"+root+"/"+user.decode("utf-8")+"/Messages/raw/"+t)
       smsprocessing.msgProcess(user.decode("utf-8"),root,hdfs)       
elif(topic == 'calllogs'):
    kvs = KafkaUtils.createDirectStream(ssc, [config[arg]['CALL_LOG']], {"metadata.broker.list": config[arg]['kafka_broker']},keyDecoder = lambda x: x,valueDecoder = lambda x: x)
    dstream =kvs.map(lambda x: (x[1]))
    s = []
    def process(rdd):
         print("rdddd",rdd.collect())
         #appended = spark.createDataFrame([("","","","","","","","","","","")],["Name","Phone","CallType","Duration","CallCreatedTime","Hash","Latitude","Longitude","Location","CellId","DeviceId"])
         for i in rdd.collect():
           nme = CallLog.CallLog.GetRootAsCallLog(i, 0).Name()
           phn = CallLog.CallLog.GetRootAsCallLog(i, 0).Phone()
           cal = CallLog.CallLog.GetRootAsCallLog(i, 0).CallType()
           dur = CallLog.CallLog.GetRootAsCallLog(i, 0).Duration()
           cct = CallLog.CallLog.GetRootAsCallLog(i, 0).CallCreatedTime()
           hash = CallLog.CallLog.GetRootAsCallLog(i, 0).Hash()
           lat = CallLog.CallLog.GetRootAsCallLog(i, 0).Latitude()
           lng = CallLog.CallLog.GetRootAsCallLog(i, 0).Longitude()
           loc = CallLog.CallLog.GetRootAsCallLog(i, 0).Location()
           cid = CallLog.CallLog.GetRootAsCallLog(i, 0).CellId()
           did = CallLog.CallLog.GetRootAsCallLog(i, 0).DeviceId()
           user = CallLog.CallLog.GetRootAsCallLog(i, 0).UserId()
           if(len(user)>0):
              #newRow = spark.createDataFrame([(nme.decode("utf-8"),phn.decode("utf-8"),cal.decode("utf-8"),dur,cct,hash.decode("utf-8"),lat,lng,loc.decode("utf-8"),cid.decode("utf-8"),did.decode("utf-8"))])
              #appended = appended.union(newRow)
              #appended.show()
              s.append((user.decode("utf-8"),nme.decode("utf-8"),phn.decode("utf-8"),cal.decode("utf-8"),dur,cct,hash.decode("utf-8"),lat,lng,loc.decode("utf-8"),cid.decode("utf-8"),did.decode("utf-8")))
         if(len(rdd.collect())>0):
              print("succeed")
              appended = spark.createDataFrame(s,["UserId","Name","Phone","CallType","Duration","CallCreatedTime","Hash","Latitude","Longitude","Location","CellId","DeviceId"])
              appended.show()
              print(user)
              t = "/"+str(int(time.time()))
              appended.write.mode('append').option("header", "true").csv(hdfs+"/"+root+"/"+user.decode("utf-8")+"/CallLogs/raw/"+t)

              
elif(topic == 'contact'):
    kvs = KafkaUtils.createDirectStream(ssc, [config[arg]['CONTACT']], {"metadata.broker.list":config[arg]['kafka_broker']},keyDecoder = lambda x: x,valueDecoder = lambda x: x)
    dstream =kvs.map(lambda x: (x[1]))
    s = []
    def process(rdd):
         print("rdddd",rdd.collect())
         #appended = spark.createDataFrame([("","","","","","")],["ContactNumber","ContactName","ContactEmail","ContactCreatedTime","Hash","DeviceId"])
         for i in rdd.collect():
           cnu = Contact.Contact.GetRootAsContact(i, 0).ContactNumber()
           cna = Contact.Contact.GetRootAsContact(i, 0).ContactName()
           cne = Contact.Contact.GetRootAsContact(i, 0).ContactEmail()
           cct= Contact.Contact.GetRootAsContact(i, 0).ContactCreatedTime()
           hash = Contact.Contact.GetRootAsContact(i, 0).Hash()
           did = Contact.Contact.GetRootAsContact(i, 0).DeviceId()
           user = Contact.Contact.GetRootAsContact(i, 0).UserId()
           if(len(cne)>0):
              #newRow = spark.createDataFrame([(cnu.decode("utf-8"),cna.decode("utf-8"),cne.decode("utf-8"),cct,hash.decode("utf-8"),did.decode("utf-8"))])
              #appended = appended.union(newRow)
              #appended.show()
              s.append((user.decode("utf-8"),cnu.decode("utf-8"),cna.decode("utf-8"),cne.decode("utf-8"),cct,hash.decode("utf-8"),did.decode("utf-8")))
         if(len(rdd.collect())>0):
              print("succeed")
              appended = spark.createDataFrame(s,["UserId","ContactNumber","ContactName","ContactEmail","ContactCreatedTime","Hash","DeviceId"])
              appended.show()
              print(user)
              t = "/"+str(int(time.time()))
              appended.write.mode('append').option("header", "true").csv(hdfs+"/"+root+"/"+user.decode("utf-8")+"/Contacts/raw/"+t)

elif(topic == 'location'):
    kvs = KafkaUtils.createDirectStream(ssc, [config[arg]['LOCATION']], {"metadata.broker.list": config[arg]['kafka_broker']},keyDecoder = lambda x: x,valueDecoder = lambda x: x)
    dstream =kvs.map(lambda x: (x[1]))
    s = []
    def process(rdd):
              print("rdddd",rdd.collect())
              #appended = spark.createDataFrame([("","","","","","")],["Latitude","Longitude","Location","LocationCaptureTime","CellId","DeviceId"])
              for i in rdd.collect():
                      lat = Location.Location.GetRootAsLocation(i, 0).Latitude()
                      lng = Location.Location.GetRootAsLocation(i, 0).Longitude()
                      loc = Location.Location.GetRootAsLocation(i, 0).Location()
                      lct = Location.Location.GetRootAsLocation(i, 0).LocationCaptureTime()
                      cid = Location.Location.GetRootAsLocation(i, 0).CellId()
                      did = Location.Location.GetRootAsLocation(i, 0).DeviceId()
                      user = Location.Location.GetRootAsLocation(i, 0).UserId()
                      if(len(user)>0):
                         #newRow = spark.createDataFrame([(lat,lng,loc.decode("utf-8"),lct,cid.decode("utf-8"),did.decode("utf-8"))])
                         #appended = appended.union(newRow)
                         #appended.show()
                         s.append((user.decode("utf-8"),lat,lng,loc.decode("utf-8"),lct,cid.decode("utf-8"),did.decode("utf-8")))
              if(len(rdd.collect())>0):
                         print("succeed")
                         appended = spark.createDataFrame(s,["UserId","Latitude","Longitude","Location","LocationCaptureTime","CellId","DeviceId"])
                         appended.show()
                         t = "/"+str(int(time.time()))
                         appended.write.mode('append').option("header", "true").csv(hdfs+"/"+root+"/"+user.decode("utf-8")+"/Location/raw/"+t)


elif(topic == 'batterystat'):
    kvs = KafkaUtils.createDirectStream(ssc, [config[arg]['BATTERY_STAT']], {"metadata.broker.list":config[arg]['kafka_broker']},keyDecoder = lambda x: x,valueDecoder = lambda x: x)
    dstream =kvs.map(lambda x: (x[1]))
    s = []
    print(config[arg]['BATTERY_STAT'])
    def process(rdd):
              print("rdddd",rdd.collect())
              #appended = spark.createDataFrame([("","","","","","","","")],["BatteryPercentage","StateOfHealth","StateOfCharge","Latitude","Longitude","Location","CellId","DeviceId"])
              for i in rdd.collect():
                      bat = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).BatteryPercentage()
                      soh = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).StateOfHealth()
                      soc = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).StateOfCharge()
                      lat = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).Latitude()
                      lng = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).Longitude()
                      loc = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).Location()
                      cid = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).CellId()
                      did = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).DeviceId()
                      user = BatteryStat.BatteryStat.GetRootAsBatteryStat(i, 0).UserId()
                      if(len(user)>0):
                         #newRow = spark.createDataFrame([(bat,soh.decode("utf-8"),soc.decode("utf-8"),lat,lng,loc.decode("utf-8"),cid.decode("utf-8"),did.decode("utf-8"))])
                         #appended = appended.union(newRow)
                         #appended.show()
                         s.append((user.decode("utf-8"),bat,soh.decode("utf-8"),soc.decode("utf-8"),lat,lng,loc.decode("utf-8"),cid.decode("utf-8"),did.decode("utf-8")))
              if(len(rdd.collect())>0):
                         print("succeed")
                         appended = spark.createDataFrame(s,["UserId","BatteryPercentage","StateOfHealth","StateOfCharge","Latitude","Longitude","Location","CellId","DeviceId"])
                         appended.show()
                         t = "/"+str(int(time.time()))
                         appended.write.mode('append').option("header", "true").csv(hdfs+"/"+root+"/"+user.decode("utf-8")+"/BatteryStat/raw/"+t)
     

elif(topic == 'appusage'):
    kvs = KafkaUtils.createDirectStream(ssc, [config[arg]['APP_USAGE']], {"metadata.broker.list": config[arg]['kafka_broker']},keyDecoder = lambda x: x,valueDecoder = lambda x: x)
    dstream =kvs.map(lambda x: (x[1]))
    s = []
    def process(rdd):
                print("rdddd",rdd.collect())
                #appended = spark.createDataFrame([("","","","","","","","","","","","","","","")],["PackageName","AppName","AppCategory","AppInstallTime","AppLaunchCount","TimeSpentInForeground","LastEventPerformed","LastOpened","AppSize","AppCacheSize","Latitude","Longitude","Location","CellId","DeviceId"])
                for i in rdd.collect():
                      pkg = AppStat.AppStat.GetRootAsAppStat(i, 0).PackageName()
                      apn = AppStat.AppStat.GetRootAsAppStat(i, 0).AppName()
                      apc = AppStat.AppStat.GetRootAsAppStat(i, 0).AppCategory()
                      ait = AppStat.AppStat.GetRootAsAppStat(i, 0).AppInstallTime()
                      alc = AppStat.AppStat.GetRootAsAppStat(i, 0).AppLaunchCount()
                      tsf = AppStat.AppStat.GetRootAsAppStat(i, 0).TimeSpentInForeground()
                      lep = AppStat.AppStat.GetRootAsAppStat(i, 0).LastEventPerformed()
                      lop = AppStat.AppStat.GetRootAsAppStat(i, 0).LastOpened()
                      aps = AppStat.AppStat.GetRootAsAppStat(i, 0).AppSize()
                      acs = AppStat.AppStat.GetRootAsAppStat(i, 0).AppCacheSize()
                      lat = AppStat.AppStat.GetRootAsAppStat(i, 0).Latitude()
                      lng = AppStat.AppStat.GetRootAsAppStat(i, 0).Longitude()
                      loc = AppStat.AppStat.GetRootAsAppStat(i, 0).Location()
                      cid = AppStat.AppStat.GetRootAsAppStat(i, 0).CellId()
                      did = AppStat.AppStat.GetRootAsAppStat(i, 0).DeviceId()
                      did = AppStat.AppStat.GetRootAsAppStat(i, 0).DeviceId()
                      user = AppStat.AppStat.GetRootAsAppStat(i, 0).UserId()
                      if(len(user)>0):
                         s.append((user.decode("utf-8"),pkg.decode("utf-8"),apn.decode("utf-8"),apc.decode("utf-8"),ait,alc,tsf,lep,lop,aps,acs,lat,lng,loc.decode("utf-8"),cid.decode("utf-8"),did.decode("utf-8")))
                         #newRow = spark.createDataFrame([(pkg.decode("utf-8"),apn.decode("utf-8"),apc.decode("utf-8"),ait,alc,tsf,lep,lop,aps,acs,lat,lng,loc.decode("utf-8"),cid.decode("utf-8"),did.decode("utf-8"))])
                         #appended = appended.union(newRow)
                         #appended.show()
                if(len(rdd.collect())>0):
                         print("succeed")
                         appended = spark.createDataFrame(s,["UserId","PackageName","AppName","AppCategory","AppInstallTime","AppLaunchCount","TimeSpentInForeground","LastEventPerformed","LastOpened","AppSize","AppCacheSize","Latitude","Longitude","Location","CellId","DeviceId"])
                         appended.show()
                         t = "/"+str(int(time.time()))
                         appended.write.mode('append').option("header", "true").csv(hdfs+"/"+root+"/"+user.decode("utf-8")+"/AppStat/raw/"+t)
                         #appended.write.csv('hdfs://159.65.158.152:9000/onionlife_development/mycsv.csv')            

elif(topic == 'deviceinfo'):
    kvs = KafkaUtils.createDirectStream(ssc, [config[arg]['DEVICE_INFO']], {"metadata.broker.list": config[arg]['kafka_broker']},keyDecoder = lambda x: x,valueDecoder = lambda x: x)
    dstream =kvs.map(lambda x: (x[1]))
    s = []
    def process(rdd):
        print("rdddd",rdd.collect())
        #appended = spark.createDataFrame([("","","","","","","","","","","","","","","","","")],["DeviceId","Phone1","Phone2","Imei1","Imei2","SerialNumber","SdkVersion","Manufacturer","Model","MobileCarrier","IsDeviceToxic","DeviceIp","AndroidVersion","Latitude","Longitude","Location","CellId"])
        for i in rdd.collect():
            did = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).DeviceId()
            pn1 = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Phone1()
            pn2 = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Phone2()
            im1 = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Imei1()
            im2 = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Imei2()
            sln = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).SerialNumber()
            sdk = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).SdkVersion()
            mnu = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Manufacturer()
            mdl = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Model()
            mbl = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).MobileCarrier()
            idt = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).IsDeviceToxic()
            dip = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).DeviceIp()
            adv = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).AndroidVersion()
            lat = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Latitude()
            lng = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Longitude()
            loc = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).Location()
            cid = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).CellId()
            user = AddDeviceInfoRequest.AddDeviceInfoRequest.GetRootAsAddDeviceInfoRequest(i, 0).UserId()
            if(len(user)>0):
                #newRow = spark.createDataFrame([(did.decode("utf-8"),pn1.decode("utf-8"),pn2.decode("utf-8"),im1.decode("utf-8"),im2.decode("utf-8"),sln.decode("utf-8"),sdk.decode("utf-8"),mnu.decode("utf-8"),mdl.decode("utf-8"),mbl.decode("utf-8"),str(idt),dip.decode("utf-8"),adv.decode("utf-8"),lat,lng,loc.decode("utf-8"),cid.decode("utf-8"))])
                #appended = appended.union(newRow)
                #appended.show()
                s.append((user.decode("utf-8"),did.decode("utf-8"),pn1.decode("utf-8"),pn2.decode("utf-8"),im1.decode("utf-8"),im2.decode("utf-8"),sln.decode("utf-8"),sdk.decode("utf-8"),mnu.decode("utf-8"),mdl.decode("utf-8"),mbl.decode("utf-8"),str(idt),dip.decode("utf-8"),adv.decode("utf-8"),lat,lng,loc.decode("utf-8"),cid.decode("utf-8")))
        if(len(rdd.collect())>0):
            print("succeed")
            appended = spark.createDataFrame(s,["UserId","DeviceId","Phone1","Phone2","Imei1","Imei2","SerialNumber","SdkVersion","Manufacturer","Model","MobileCarrier","IsDeviceToxic","DeviceIp","AndroidVersion","Latitude","Longitude","Location","CellId"])
            appended.show()
            t = "/"+str(int(time.time()))
            appended.write.mode('append').option("header", "true").csv(hdfs+"/"+root+"/"+user.decode("utf-8")+"/DeviceInfo/raw/"+t)



dstream.foreachRDD(lambda x:process(x))
offsetRanges = []

def storeOffsetRanges(rdd):
     global offsetRanges
     offsetRanges = rdd.offsetRanges()
     return rdd

def printOffsetRanges(rdd):
     for o in offsetRanges:
         print ("%s %s %s %s",(o, o.topic, o.partition, o.fromOffset, o.untilOffset))

kvs.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)

'''
if(kvs):
    dstream =kvs.map(lambda x: (x[1]))
    dstream.foreachRDD(lambda x:process(x))
elif(kvs1):
    dstream1 =kvs1.map(lambda x: (x[1]))
    dstream1.foreachRDD(lambda x:process1(x))
elif(kvs2):
    dstream2 =kvs2.map(lambda x: (x[1]))
    dstream2.foreachRDD(lambda x:process2(x))
elif(kvs3):
    dstream3 =kvs3.map(lambda x: (x[1]))
    dstream3.foreachRDD(lambda x:process3(x))
elif(kvs4):
    dstream4 =kvs4.map(lambda x: (x[1]))
    dstream4.foreachRDD(lambda x:process4(x))
elif(kvs5):
    dstream5 =kvs5.map(lambda x: (x[1]))
    dstream5.foreachRDD(lambda x:process5(x))
else:
    sleep(10)
    ssc.start()
    '''
ssc.start()
ssc.awaitTermination()
#ssc.stop(stopSparkContext=True, stopGraceFully=True)

