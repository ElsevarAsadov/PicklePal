import os
import logging
import datetime
import csv
from pathlib import Path
from typing import Callable
from uuid import uuid4
from dill import dump, load

class Pickler:

  HOME_PATH = str(Path.home())
  DEFAULT_PATH = os.path.join(HOME_PATH, "Appdata", "Roaming", "PicklePal")
  
  os.chdir(HOME_PATH)
  
  if not os.path.isdir(DEFAULT_PATH):
    os.makedirs(DEFAULT_PATH)
  #Time for the object is created.
  START_TIME = str(datetime.datetime.utcnow().isoformat().replace(":", "-"))
  
  #HOME_PATH + log_dir
  log_dir = "PicklePal Logs"
  #creating logs folder
  if not os.path.isdir("PicklePal Logs"):
    os.makedirs("PicklePal Logs")
    
  LOGGER = logging.Logger(__name__)
  
  FILE_LOG = logging.FileHandler(filename=log_dir+f"\\{START_TIME}.log")

  LOGGER.setLevel(logging.DEBUG)
  FILE_LOG.setLevel(logging.DEBUG)
  
  LOG_FORMAT = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
  FILE_LOG.setFormatter(LOG_FORMAT)
  
  LOGGER.addHandler(FILE_LOG)
  
  def __init__(self, temp_folder=DEFAULT_PATH):
    #Pickler properties.
    self.temp_folder = temp_folder
    
    
    #Creating temp folders.
    self.__create_enviroment()
    
  def __create_enviroment(self)->None:
    """
    Creates 3 folders which we will save pickle objects there.
    
    """
    
    self.LOGGER.info(f"Temp files are being created in {self.temp_folder}.")
 
    for folder in ["WAITING", "RUNNING", "DONE", "TASKS"]:
      try:
        os.makedirs(os.path.join(self.temp_folder, folder))
      except FileExistsError:
        self.LOGGER.warning(f"{folder} is already in temp dir : {self.temp_folder}")
        
    self.LOGGER.info(f"Temp files are created SUCCESSFULLY in {self.temp_folder}.")
    
    self._csv_dir = os.path.join(self.temp_folder, "TASKS")
    
    self.LOGGER.info(f"CSV file is creating in {self._csv_dir}")
    
    if not os.path.isfile(os.path.join(self._csv_dir, "tasks.csv")):
      with open(os.path.join(self._csv_dir, "tasks.csv"), "a") as f:
        writer = csv.writer(f)
      
        writer.writerow(["TASK", "ID", "STATUS", "TIME"])
    
    self.LOGGER.info(f"CSV file is created SUCCESSFULLY.. in {self._csv_dir}.")
    
  def __loader(self):
    loads = {}
    for task in self.__tasks:
      t = float(datetime.datetime.now().timestamp())
      print("t", t)
      if t >= float(task["TIME"]):
        id = task["ID"]
        name = task["TASK"]
        
        #loading
        with open (os.path.join(self.temp_folder, "WAITING", id+".pkl"), "rb") as f:
          k = load(f)
        
        loads[name] = k
    
    return loads
    
  def register(self, fn:Callable, *, tname=None):
    """This function registering function to waiting folder.
    function should 'pickleable' type object."""
   
    #if function name is not specified in registering.
    if not tname:
      tname = fn.__name__
      
    #there is 2^122 chance to collision
    id = str(uuid4())
      
    with open(os.path.join(self.temp_folder, "WAITING", f"{id}.pkl"), "wb") as pkl, \
         open(os.path.join(self.temp_folder, "TASKS", f"tasks.csv"), "a", newline="") as cv:
          
        writer = csv.writer(cv)
          
        #save pickled data into temp folder.
        self.LOGGER.info(f"{fn.__name__} is dumping... in {os.path.join(self.temp_folder, 'WAITING')}.")
        dump(obj=fn, file=pkl)
        
        self.LOGGER.info(f"{fn.__name__} is dumped SUCCESSFULLY... in {os.path.join(self.temp_folder, 'WAITING')}.")
        
        #save csv file into temp folder.
        self.LOGGER.info(f"{fn.__name__} is dumping... in {os.path.join(self.temp_folder, 'WAITING')}.")
        
        #time now
        time_now = datetime.datetime.now().timestamp()
        
        #writing datas to csv
        writer.writerow([tname, id, "WAITING", time_now])
        
        self.LOGGER.info(f"{fn.__name__} is dumped SUCCESSFULLY... in {os.path.join(self.temp_folder, 'WAITING')}.")
  
  def listener(self, task_path=None):
    """This function executes tasks"""
    
    if not task_path:
      task_path = self._csv_dir
    
    path = os.path.join(task_path, "tasks.csv")
    
    with open(path, newline="") as f:
      reader = csv.DictReader(f)
      
      self.__tasks = [row for row in reader]
    
