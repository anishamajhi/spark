import subprocess
from threading import Thread
import sys
import configparser
def sms():
      command = "spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar process1.py development sms"
      p = subprocess.Popen(command , shell=True, stdout=subprocess.PIPE).wait()
      
def contacts():
    command = "spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar process1.py development contact"
    p = subprocess.Popen(command , shell=True, stdout=subprocess.PIPE).wait()
  
def calllogs():
    command = "spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.0.jar process1.py development calllog"
    p = subprocess.Popen(command , shell=True, stdout=subprocess.PIPE).wait()
  
if __name__ == '__main__':
      Thread(target = contacts()).start()
      Thread(target = calllogs()).start()
