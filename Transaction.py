from enum import Enum
import logging
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

class TransactionStatus(Enum):
    ABORTED = "ABORTED"
    RUNNING = "RUNNING"
    COMMITTED = "COMMITTED"
    WAITING = "WAITING"

class TransactionType(Enum):
    READ = "READ"
    WRITE = "WRITE"
    UNDEFINED = "UNDEFINED"

class Transaction:
    def __init__(self, id, name, timestamp):
        self.id = id
        self.name = name
        self.status = TransactionStatus.RUNNING
        self.type = TransactionType.UNDEFINED
        self.arrival_time = timestamp
        self.commit_time = None
        self.sites_accessed = []
        self.pre_commit_vars = {}

    #Getter functions  
    def get_id(self):
        """Returns the unique ID of the transaction"""
        return self.id
    
    def get_arrival_time(self):
        """Retrieves the arrival time of the transaction"""
        return self.arrival_time
    
    def get_commit_time(self):
        """Retrieves the arrival time of the transaction"""
        return self.commit_time
    
    def get_name(self):
        """Returns the name of the transaction"""
        return self.name
    
    def get_transaction_status(self):
        """Returns the current status of the transaction (e.g. RUNNING, COMMITTED, ABORTED)"""
        return self.status
    
    def get_transaction_type(self):
        """Returns the type of the transaction (either READ or WRITE)"""
        return self.type
    
    def get_sites_accessed(self):
        """Returns list of sites that transaction was accessed"""
        return self.sites_accessed
    
    def get_local_variables(self):
        """Returns list of sites that transaction was accessed"""
        return self.local_variables
    
    def get_precommit_variables(self):
        """Returns list of sites that transaction was accessed"""
        return self.pre_commit_vars
    
    
    #Setter functions

    def set_status(self, status):
        """Sets the status of the transaction to a specified state"""
        self.status = status

    def set_type(self, type):
        """Sets the type of the transaction, either read or write"""
        self.type = type
    
    def set_commit_time(self, commit_time):
        """Sets the commit time of the transaction"""
        self.commit_time = commit_time
    
    def add_site_accessed(self, site_id):
        """Records that a specific site was accessed by the transaction at a given time"""
        self.sites_accessed.append(int(site_id))
    
    def add_precommit_variables(self, var_idx, value):
        """Adds a var_idx:value pair for every pre-commit var"""
        self.pre_commit_vars[str(var_idx)] = value

    def display(self):
        """Helper function to display transaction details"""
        print(f"Transaction ID: {self.id}")
        print(f"Arrival Time: {self.arrival_time}")
        print(f"Name: {self.name}")
        print(f"Status: {self.status}")
        print(f"Commit Time: {'N/A' if self.commit_time == -1 else self.commit_time}")
        print(f"Type: {self.type}")
        print(f"Sites Accessed: {', '.join(map(str, self.sites_accessed))}")
        
    