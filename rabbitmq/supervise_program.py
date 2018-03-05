import threading
import time
import os
import subprocess
def get_process_count(imagename):
  p = os.popen('tasklist /FI "IMAGENAME eq %s"' % imagename)
  return p.read().count(imagename)
def timer_start():
  t = threading.Timer(120,watch_func,("is running..."))
  t.start()
def watch_func(msg):
  print("I'm watch_func,",msg)
  if get_process_count('main.exe') == 0 :
    print(os.popen(r'python3 /search/zhangxiaopeng/TaskDistribution/rabbitmq/supervise.py'))

if __name__ == "__main__":
  # timer_start()
  watch_func("is running.")
  # while True:
  #   time.sleep(1)