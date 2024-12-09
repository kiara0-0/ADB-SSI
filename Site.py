import logging
from enum import Enum
from Variable import Variable
from DataManager import DataManager
"""
       Authors: Krina KJS10093
       Chynna
"""
log_filename = f"app.log"
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

class SiteStatus(Enum):
    UP = "UP"
    FAILED = "FAILED"
    RECOVERED = "RECOVERED"

class Site:
    def __init__(self, idx):
        self.id=idx
        self.status=SiteStatus.UP #initally the sites are all up
        self.last_failure_time=None
        self.datamanager=DataManager(self.id)           

    def get_id(self):
        return self.id
    
    def getSiteDetails(self):
        return self.id,self.status,self.last_failure_time,self.variables
    
    def getSiteStatus(self):
        return self.status

    def setStatusOfSite(self,stat):
        if stat in SiteStatus:
            self.status=stat
            log.info(f"Site {self.id} status changed to {stat}")
        else:
            print("Not a valid status")
        return
    
    def getNumberVariables(self):
        return len(self.variables)
    
    def fail(self):
        self.status="FAILED"

    def recover(self):
        self.status="RECOVERED"

    def getLastFailureTime(self):
        return self.last_failure_time
    
    def setLastFailureTime(self,time):
        self.last_failure_time=time

    def displaySite(self):
        print("site id is: " + str(self.id))
        print("site status is: " + str(self.status))
        print("The variables on this site are: ")
        self.datamanager.getVariableList()
    
    def getDataManager(self):
        return self.datamanager
    