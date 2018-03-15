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


def get_proc_memory(proc_name):
  memory = 0

  task = os.popen('tasklist')
  task_content = task.read()

  MAX_IMAGENAME_LEN = 25
  proc_name_show = proc_name
  if len(proc_name_show) > MAX_IMAGENAME_LEN:
    proc_name_show = proc_name_show[:MAX_IMAGENAME_LEN]
  if task_content.find(proc_name_show) > -1:
    try:
      num = task_content.split().index(proc_name_show)
      tasklist = task_content.split()
      index_n = num + 1
      memory = tasklist[index_n + 3]
    except Exception as e:
      print(e)
    # break
  return memory

if __name__ == "__main__":
  # timer_start()
  # watch_func("is running.")
  import subprocess
  res = subprocess.check_output(['ps'])
  print(res)
  # while True:
  #   time.sleep(1)