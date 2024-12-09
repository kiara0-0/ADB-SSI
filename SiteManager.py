import logging
from Site import Site
from Site import SiteStatus
from collections import defaultdict
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

class SiteManager:
    def __init__(self, num_sites):
        self.num_sites = num_sites
        self.site_failure_history = {i: [0] for i in range(1, num_sites + 1)}
        self.site_recover_history = {i: [0] for i in range(1, num_sites + 1)}
        self.waitingEvenTxn = defaultdict(list)
        self.waitingOddTxn = defaultdict(list)
        self.sites = self.initializeSites()

    def initializeSites(self):
        sites = []
        for i in range(1,11):
            sites.append(Site(i))
        return sites
    
    def getNumberSites(self):
        return len(self.sites)
    
    def getSite(self,index):
        return self.sites[index]
    
    def getAllSites(self):
        return self.sites
    
    def getSiteStatus(self,index): 
        site = self.getSite(index)
        return site.getSiteStatus()
    
    def display(self):
        pass
        # for site in self.sites:
            # site.displaySite()

    def failSite(self,id):
        self.sites[int(id)-1].setStatusOfSite(SiteStatus.FAILED)

    def recoverSite(self,id):
        self.sites[int(id)-1].setStatusOfSite(SiteStatus.RECOVERED)

    def addRecoveredSiteToList(self,id,time):
        site_id = int(id)
        if site_id in self.site_recover_history:
            self.site_recover_history[site_id].append(time)
        else:
        # If the site_id is not in the dictionary, initialize its history with the time
            self.site_recover_history[site_id] = [time]
    
    def dump(self):
        """Print the committed values of all variables at all sites."""
        log.info("Dumping all site states...")
        for site in self.sites:
            site_id = site.id
            site_status = site.getSiteStatus()
            log.info(f"Site {site_id} (Status: {site_status}):")
            
            #grab variables from the data manager
            data_manager = site.getDataManager()
            committed_variables = data_manager.getVariableList()
            
            # Check if committed_variables is a list or dictionary
            if isinstance(committed_variables, dict):
                for var_id, variable in committed_variables.items():
                    var_name = f"x{var_id}"  # Convert variable ID to string with "x" prefix
                    # value = variable.getVariableValue()
                    value=variable.snapshots[-1][1]
                    log.info(f"  {var_name}: {value}")
            elif isinstance(committed_variables, list):
                for variable in committed_variables:
                    var_name = variable.getVariableName()  # Assuming a method to get variable name
                    # value = variable.getVariableValue()  # Assuming a method to get variable value
                    value=variable.snapshots[-1][1]
                    log.info(f"  {var_name}: {value}")
            else:
                log.warning(f"Unrecognized type for committed variables at site {site_id}: {type(committed_variables)}")
    
    def add_waitlist_txn_even(self,site_id, txn_obj, var_index):
        if site_id not in self.waitingEvenTxn:
            self.waitingEvenTxn[site_id] = []
        self.waitingEvenTxn[site_id].append((txn_obj,var_index))
        log.debug(f"Added transaction {txn_obj.get_id()} to even waitlist at site {site_id} for variable {var_index}")

    def add_waitlist_txn_odd(self,site_id, txn_obj, var_index):
        if site_id not in self.waitingOddTxn:
            self.waitingOddTxn[site_id] = []
        self.waitingOddTxn[site_id].append((txn_obj,var_index))
        log.debug(f"Added transaction {txn_obj.get_id()} to odd waitlist at site {site_id} for variable {var_index}")

    def get_site_failure_history(self, index):
        """
        Returns the number of times a site recovered
        """
        return self.site_failure_history[index]

    def get_site_recover_history(self, index):
        """
        Returns the number of times a site recovered
        """
        return self.site_recover_history[index]

    def get_sites_holding_variable(self, variable_index):
        """
        Returns a list of sites that hold the specified variable.
        """
        holding_sites = []
        for site in self.sites:
            if site.getDataManager().has_variable(variable_index):
                holding_sites.append(site)
        return holding_sites
