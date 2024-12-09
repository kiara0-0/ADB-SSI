import logging
from Variable import Variable
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

class DataManager:
    def __init__(self,id):
        self.current_site=id #stores the site that the data manager is present in
        self.committed_variables=self.populateVariables()
        #log.debug(f"Committed variables for site {self.current_site}: {[var.getVariableName() for var in self.committed_variables]}")
        #Create separate copies for replicated and pre-committed variables
        self.replicated_variables = [var for var in self.committed_variables]
        self.pre_committed_variables = [var for var in self.committed_variables]
        #log.debug(f"Pre-committed variables for site {self.current_site}: {[var.getVariableName() for var in self.pre_committed_variables]}")


    def populateVariables(self):
        variables = []
        for i in range(1, 21):  # Variable IDs from x1 to x19
            key = f"x{i}"
            value = 10 * i
            if i % 2 == 0:  # Even-indexed variables are replicated across all sites
                variables.append(Variable(key, self.current_site, value))
                #log.debug(f"Variable {key} initialized at site {self.current_site} with value {value} (Even-indexed).")
            else:  # Odd-indexed variables are hosted at one site
                if self.current_site == (i % 10 + 1):  # Ensure only the designated site hosts the variable
                    variables.append(Variable(key, self.current_site, value))
                    #log.debug(f"Variable {key} initialized at site {self.current_site} with value {value} (Odd-indexed).")
                #else:
                    #log.debug(f"Variable {key} skipped initialization at site {self.current_site} (Odd-indexed).")
        return variables

    def getVariableList(self):
        return self.committed_variables
    
    def getPreCommittedVariablesList(self):
        return self.pre_committed_variables
    
    def updateVariableValue(self,var_name,value):
        for variable in self.committed_variables:
            if var_name==variable.getVariableName():
                variable.setVariableValue(value)

    
    def findRecentSnapshot(self, txn_start_time, var_idx):
        """Finds the most recent snapshot of a variable before a given transaction start time."""
        recent_snapshot = None
        for variable in self.committed_variables:
            if int(variable.getVariableName()[1:]) == var_idx:
                # recent_snapshot_list=variable.get_snapshots_list()
                recent_snapshot_value=variable.find_snapshot_before_time(txn_start_time)
                recent_snapshot_time=variable.most_recent_snapshot_time()
                # commit_time = variable.getCommitTime()
                # if commit_time is None or commit_time < txn_start_time:
                    # return variable
                    # if recent_snapshot is None or (commit_time and recent_snapshot.getCommitTime() < commit_time):
                    #     recent_snapshot = variable
                if recent_snapshot_value:
                    log.debug(f"Found recent snapshot for x{var_idx} with value {recent_snapshot_value} and commit time {recent_snapshot_time}.")
                else:
                    log.warning(f"No valid snapshot found for x{var_idx}.")

                return recent_snapshot_value

    def update_local_copy(self, var_idx, value, txn_obj):
        log.debug(f"Attempting update local copy for x{var_idx} at site {self.current_site}.")
        log.debug(f"Pre-committed variables at site {self.current_site}: {[var.getVariableName() for var in self.pre_committed_variables]}")
        """Tentatively writes a value to the pre-commit buffer."""
        log.debug(f"Attempting update local copy for x{var_idx} at site {self.current_site}.")

        txn_obj.add_precommit_variables(var_idx, value)
        for variable in self.pre_committed_variables:
            if variable.getVariableID() == var_idx:
                variable.value = value
                # variable.setCommitTime(txn_obj.get_arrival_time())
                log.debug(f"Update local copy succeeded for x{var_idx} with value {value} at site {self.current_site}.")
                return True
        log.warning(f"update local copy failed: x{var_idx} not found in pre-committed variables at site {self.current_site}.")
        return False
 
    def abort_transaction(self, txn_obj):
        """Removes all updated local copies for a transaction."""
        self.pre_committed_variables = [
            v for v in self.pre_committed_variables if (not v.getCommitTime()) or (v.getCommitTime() < txn_obj.get_arrival_time())
        ]
        # temp_variables = []
        # for v in self.pre_committed_variables:
        #     if (v.getCommitTime()):
        #         if(v.getCommitTime() < txn_obj.get_arrival_time()):
        #             temp_variables.append(v)
        # self.pre_committed_variables = temp_variables
        log.debug(f"Cleaned up update local copys for transaction {txn_obj.get_name()}.")

    def checkCommitBtwTimeRange(self,recovery_time, txn_arrival_time, var_id):
        """
        Checks the commit between the time range
        """
        for variable in self.committed_variables:
            if variable.getVariableName()[1:]==str(var_id):
                variable_snapshots = variable.get_snapshots_list()

                if len(variable_snapshots)==1:
                    return True
                else:
                    for time,val in variable_snapshots:
                        if time > recovery_time and time < txn_arrival_time:
                            return True

        return False
    
    def has_variable(self, variable_index):
        """
        Checks if the variable with the given index is stored in this data manager.
        Iterates through the list of committed variables to find a match.
        """
        var_name = f"x{variable_index}"
        for variable in self.committed_variables:
            if variable.getVariableName() == var_name:
                return True
        return False
    
    def getVariable(self, var_name):
        """ Retrieves a variable object by its name. """
        for variable in self.committed_variables:
            if variable.getVariableName() == var_name:
                return variable
        return None

    
    def commit_variable(self, var_name, commit_time, txn_obj):
        """
        Commits changes to the variable by transferring from pre_committed_variables to committed_variables.
        Args:
            var_name (str): The name of the variable to commit.
            commit_time (datetime): The time at which the commit is made.
        """
        # Find the variable in pre-committed and update it in committed variables
        for variable in self.pre_committed_variables:
            if variable.getVariableName() == var_name:
                # Assuming Variable class has setCommitTime method
                variable.setCommitTime(commit_time)
                if(var_name[1:] in txn_obj.pre_commit_vars):
                    variable.setVariableValue(txn_obj.pre_commit_vars[var_name[1:]])
                # Move the variable from pre-committed to committed
                index = self.committed_variables.index(variable)
                self.committed_variables[index] = variable
                log.debug(f"Committed {var_name} at time {commit_time} in site {self.current_site}")
                return True
        
        log.warning(f"Variable {var_name} not found in pre-committed for committing at site {self.current_site}.")
        return False
