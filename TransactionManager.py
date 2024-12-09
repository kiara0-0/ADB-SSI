import logging
from Transaction import Transaction
from SiteManager import SiteManager
from DataManager import DataManager
from collections import defaultdict
from Site import SiteStatus
from Transaction import TransactionStatus
from Transaction import TransactionType
import copy
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

class TransactionManager:
    def __init__(self, num_variables, num_sites, site_manager):
        """
        Initializes the TransactionManager with:
        - Active transactions map (`txn_map`).
        - Serialization graph (`serialization_graph`) for conflict tracking.
        - Access history (`txn_access_hist`) to track read/write operations.
        - SiteManager instance to manage site-related operations.
        """
        self.txn_map = {}
        self.serialization_graph = defaultdict(list)
        self.txn_access_hist = defaultdict(lambda: defaultdict(list))
        self.site_manager = site_manager
        self.num_variables = num_variables
        self.num_sites = num_sites
        self.current_time = 0
        self.V = 0

    def begin_transaction(self, txn_name, current_time):
        """
        Starts a new transaction, initializing its metadata and adding it to the active map.
        Logs a warning if the transaction already exists.
        """
        if txn_name in self.txn_map:
            log.warning(f"Transaction {txn_name} already exists!")
            return

        txn_id = int(txn_name[1:])
        transaction = Transaction(txn_id, txn_name, current_time)
        self.txn_map[txn_name] = transaction
        self.add_node(txn_id)
        log.debug(f"Transaction {txn_name} begins at time {current_time}:")

    def read_request(self, txn_name, variable, current_time):
        """
        Handles a read request by:
        1. Checking for read-write (rw) conflicts and adding edges.
        2. Detecting cycles in the serialization graph.
        3. Attempting to read the variable based on its index (even/odd).
        """
        if txn_name not in self.txn_map:
            log.error("Read request denied: Transaction %s does not exist at time %s.", txn_name, current_time)
            return

        txn_obj = self.txn_map[txn_name]
        var_idx = int(variable[1:])
        log.info("Processing read request for transaction %s and variable %s at time %s.", txn_name, variable, current_time)

        if txn_obj.get_transaction_type() == TransactionType.UNDEFINED:
            txn_obj.set_type(TransactionType.READ)

        #Add the read operation to the transaction's access history
        self.txn_access_hist[txn_obj.get_id()][var_idx].append("R")
        log.debug(f"Updated access history for T{txn_obj.get_id()} on x{var_idx}: Read")
        log.debug(f"Current access history after {txn_name}: {dict(self.txn_access_hist)}")

        #Delegate to appropriate handler
        if self.is_even_index(var_idx):
            self.handle_even_indexed_variable(txn_obj, variable, var_idx,current_time)
        else:
            self.handle_odd_indexed_variable(txn_obj, variable, var_idx,current_time)

    def write_request(self, txn_name, variable, value, current_time):
        """
        Handles a write request by:
        1. Checking for conflicts (rw, ww) and adding serialization graph edges.
        2. Detecting cycles in the serialization graph.
        3. Attempting a update to local copy to the appropriate sites.
        4. Aborting or logging success based on the write outcome.
        """
        if txn_name not in self.txn_map:
            log.error(f"Write request denied: Transaction {txn_name} does not exist.")
            return

        txn_obj = self.txn_map[txn_name]
        txn_id = txn_obj.get_id()
        var_idx = int(variable[1:])
        log.info(f"Processing write request for transaction {txn_name}, variable {variable} with value {value} at time {current_time}")

        if txn_obj.get_transaction_type() == TransactionType.UNDEFINED:
            txn_obj.set_type(TransactionType.WRITE)

        self.txn_access_hist[txn_id][var_idx].append("W")
        log.debug(f"Updated access history for T{txn_obj.get_id()} on x{var_idx}: Write")
        log.debug(f"Current access history after {txn_name}: {dict(self.txn_access_hist)}")

        #Attempt update local copy
        if self.attempt_write(txn_obj, var_idx, value):
            log.info(f"Transaction {txn_name} successfully attempted a write on variable {variable}.")
        else:
            log.error(f"Transaction {txn_name} failed to update for variable {variable}. Aborting transaction.")
            self.abort_transaction(txn_name, current_time)

    def end_transaction(self, txn_name, current_time):
        """
        Completes a transaction by committing if no cycles are detected.
        Aborts if a cycle is detected before cleanup or other conditions for abort are met.
        """
        log.info("Txn %s: END. Checking whether to COMMIT/ABORT...", txn_name)

        if txn_name not in self.txn_map:
            log.warning(f"Transaction {txn_name} does not exist.")
            return

        txn_obj = self.txn_map[txn_name]
        txn_id = txn_obj.get_id()
        txn_start_time = txn_obj.get_arrival_time()
        log.info("Txn %s: Transaction status is %s", txn_name, txn_obj.get_transaction_status())

        if txn_obj.get_transaction_status() == TransactionStatus.WAITING:
            log.info("Txn %s: is waiting on some read. Must be ABORTED", txn_name)
            self.abort_transaction(txn_name, current_time)
            return

        #Case 1: Check for site failure after write
        log.debug("Accessed Sites: %s", txn_obj.get_sites_accessed())
        accessed_sites = txn_obj.get_sites_accessed() 
        for site_id in accessed_sites:
            operation = txn_obj.get_transaction_type() 
            timestamp = txn_obj.get_arrival_time()

            if operation == TransactionType.WRITE:
                failure_history = self.site_manager.get_site_failure_history(site_id)
                for fail_time in failure_history:
                    if fail_time > timestamp:
                        log.info("Txn %s: ABORTED due to site failure after write.", txn_name)
                        self.abort_transaction(txn_name, current_time)
                        return
                    
        if txn_obj.get_transaction_status() != TransactionStatus.ABORTED:
            #Case 2: Check for Snapshot Isolation violations
            variables_accessed = self.txn_access_hist[txn_id]
            log.debug(f"Checking SSI violations for Txn {txn_name} on accessed variables: {variables_accessed}")
            for var_idx, operations in variables_accessed.items():
                if 'W' in operations:
                    log.debug(f"Txn {txn_name} has a write operation on variable x{var_idx}, checking against other transactions...")
                    target_sites = self.site_manager.get_sites_holding_variable(var_idx)

                    for site in target_sites:
                        data_manager = site.getDataManager()
                        variable = data_manager.getVariable(f"x{var_idx}") 
                        last_committed_time = variable.getCommitTime() if variable else None
                        if last_committed_time and last_committed_time > txn_start_time:
                            log.info(f"Txn {txn_name}: ABORTED due to a later write from another transaction on variable x{var_idx} at site {site.get_id()}.")
                            self.abort_transaction(txn_name, current_time)
                            return

            #Case 3: Check for cycles in the serialization graph
            # Backup the current serialization graph
            backup_graph = copy.deepcopy(self.serialization_graph)
            
            # Add relevant edges
            self.add_edges_based_on_access(txn_id, variables_accessed)

            culprit_txn_id = self.is_cyclic()
            if culprit_txn_id:
                log.info(f"Txn T{culprit_txn_id}: ABORTED due to a cycle in the serialization graph.")
                self.abort_transaction(f"T{culprit_txn_id}", current_time)
            
            if txn_obj.get_transaction_status() != TransactionStatus.ABORTED:
                self.commit_transaction(txn_obj, current_time)
                txn_obj.set_commit_time(current_time)
                log.info(f"Txn {txn_name}: COMMITTED SUCCESSFULLY.")

    def add_edges_based_on_access(self, txn_id, variables_accessed):
        """
        Adds edges to the serialization graph based on transaction accesses and timing.
        This updated function includes edges for active transactions to accurately reflect potential conflicts.
        """
        log.debug(f"Adding edges for Transaction {txn_id} based on accessed variables: {variables_accessed}")

        for var_idx, operations in variables_accessed.items():
            w_flag = "W" in operations
            r_flag = "R" in operations
            for other_txn_name, other_txn_obj in self.txn_map.items():
                other_txn_id = int(other_txn_name[1:])
                if other_txn_id == txn_id:
                    continue  # Skip self-edges

                other_vars_accessed = self.txn_access_hist[other_txn_id]
                log.debug(f"Access history for {txn_id}: {self.txn_access_hist[txn_id]} before adding edges")
                log.debug(f"Access history for {other_txn_id}: {self.txn_access_hist[other_txn_id]} before adding edges")
                other_w_flag = "W" in other_vars_accessed.get(var_idx, [])
                log.debug(f"Evaluating other_w_flag for {other_txn_id} on x{var_idx}: {other_vars_accessed.get(var_idx, [])} -> {other_w_flag}")
                other_r_flag = "R" in other_vars_accessed.get(var_idx, [])
                log.debug(f"Evaluating other_r_flag for {other_txn_id} on x{var_idx}: {other_vars_accessed.get(var_idx, [])} -> {other_r_flag}")

                # Log other transaction details
                log.debug(f"Comparing with {other_txn_id}: {'Write' if other_w_flag else ''} {'Read' if other_r_flag else ''} on x{var_idx}")

                # Consider all potential conflicts, not just with committed transactions
                if other_w_flag and w_flag:
                    log.info(f"Conflict detected: {other_txn_id} writes to x{var_idx} and {txn_id} also writes. Adding ww edge.")
                    self.add_edge(txn_id, other_txn_id, 'ww')
                if other_w_flag and r_flag:
                    log.info(f"Conflict detected: {other_txn_id} writes to x{var_idx} and {txn_id} reads. Adding wr edge.")
                    self.add_edge(other_txn_id, txn_id, 'wr')
                if other_r_flag and w_flag:
                    log.info(f"Conflict detected: {other_txn_id} reads x{var_idx} and {txn_id} writes. Adding rw edge.")
                    self.add_edge(txn_id, other_txn_id, 'rw')

        # Log the serialization graph after adding edges
        log.debug("Serialization graph after adding edges:")
        self.print_serialization_graph()

    def add_edge(self, u, v, edge_type=None):
        """
        Adds an edge from node u to node v in the serialization graph.
        Optionally, includes an edge type for conflict classification.
        """
        if u not in self.serialization_graph:
            self.serialization_graph[u] = set()

        # Check if the edge already exists
        existing_edges = {neighbor for neighbor in self.serialization_graph[u]}
        if v in existing_edges:
            log.debug(f"Edge T{u} -> T{v} already exists. Skipping.")
        else:
            self.serialization_graph[u].add((v, edge_type))
            log.debug(f"Added edge T{u} -> T{v} of type {edge_type}.")

        # Print updated graph
        self.print_serialization_graph()

    def is_cyclic_util(self, v, visited, rec_stack, path, cycle_path):
        """
        Helper function for cycle detection in a directed graph.
        Marks visited nodes and detects cycles using recursion.
        """
        visited[v] = True
        rec_stack[v] = True
        path.append(v)

        for neighbor, _ in self.serialization_graph.get(v, []):
            if not visited[neighbor]:
                if self.is_cyclic_util(neighbor, visited, rec_stack, path, cycle_path):
                    return True
            elif rec_stack[neighbor]:
                # Cycle detected
                path.append(neighbor)
                cycle_path.extend(path)
                return True

        rec_stack[v] = False
        path.pop()
        return False


    def is_cyclic(self):
        """
        Checks if the serialization graph contains any cycles and returns the transaction causing the cycle.
        """
        visited = {node: False for node in self.serialization_graph}
        rec_stack = {node: False for node in self.serialization_graph}
        cycle_path = []

        for node in self.serialization_graph:
            if not visited[node]:
                if self.is_cyclic_util(node, visited, rec_stack, [], cycle_path):
                    # Sort cycle_path by transaction start time and return the latest transaction
                    log.debug(f"Cycle path detected: {cycle_path}")
                    latest_txn = max(cycle_path, key=lambda txn: self.txn_map[f'T{txn}'].get_arrival_time())
                    log.warning(f"Cycle detected caused by transaction {latest_txn}.")
                    return latest_txn

        return None  # No cycle detected
    
    def handle_odd_indexed_variable(self, txn_obj, var_name, var_idx, current_time):
        """
        Handles read requests for odd-indexed variables by:
        1. Identifying the target site based on the variable index.
        2. Attempting to serve the read from the target site, prioritizing:
        - Active (UP) sites.
        - Recovered sites with valid recent snapshots.
        3. Processing the read failure if no valid site is available.
        """
        target_site_id = 1 + var_idx % 10

        #Iterate over all sites to find the target site
        for site in self.site_manager.getAllSites():
            if site.get_id() == target_site_id:
                if site.getSiteStatus() == SiteStatus.UP:
                    #Check if the site can serve the read request
                    if self.can_site_serve_read(site, txn_obj.get_name(), var_idx):
                        self.process_read_success(site, txn_obj, var_name, var_idx)
                        return
                elif site.getSiteStatus() == SiteStatus.RECOVERED:
                    #Check for valid committed writes after recovery
                    recovery_history = self.site_manager.get_site_recover_history(site.get_id())
                    last_recovery_time = max(recovery_history, default=float('-inf'))

                    if self.can_site_serve_read(site, txn_obj.get_name(), var_idx):
                        if site.getDataManager().checkCommitBtwTimeRange(last_recovery_time, txn_obj.get_arrival_time(), var_idx):
                            self.process_read_success(site, txn_obj, var_name, var_idx)
                            return
                        
                    log.debug("Site %s has recovered but no valid write for variable %s.", site.get_id(), var_name)

                else:
                    #Handle unavailable site
                    log.error("Transaction %s failed to read variable %s from site %s. Site unavailable in %s state.",
                            txn_obj.get_name(), var_name, site.get_id(), site.getSiteStatus())
                    break

        #If no valid site was found, process the read failure
        log.error("Transaction %s failed to read variable %s from site %s. Site unavailable.",
                txn_obj.get_name(), var_name, target_site_id)
        self.process_read_failure(txn_obj, var_name)

    def handle_even_indexed_variable(self, txn_obj, var_name, var_idx, current_time):
        """
        Handles read requests for even-indexed variables by:
        1. Iterating over all sites that can serve even-indexed variables.
        2. Attempting to serve the read from UP or RECOVERED sites.
        3. Adding the transaction to a waitlist if no sites can serve the read request.
        """
        sites_to_wait = []

        for site in self.site_manager.getAllSites():
            log.debug("Checking site %s in %s state for variable %s (Transaction %s)",
                    site.get_id(), site.getSiteStatus(), var_name, txn_obj.get_name())

            #Skip invalid sites for even-indexed variables
            #if var_idx % 2 == 0 and site.get_id() % 2 != 0:
            #    log.debug("Skipping site %s as its ID is not valid for even-indexed variable %s.", site.get_id(), var_name)
            #    continue

            if site.getSiteStatus() == SiteStatus.UP:
                #Check if the site can serve the read request
                if self.can_site_serve_read(site, txn_obj.get_name(), var_idx):
                    self.process_read_success(site, txn_obj, var_name, var_idx)
                    return
            elif site.getSiteStatus() == SiteStatus.RECOVERED:
                #Check for valid committed writes after recovery
                recovery_history = self.site_manager.site_recover_history.get(site.get_id(), []) 
                last_recovery_time = max(recovery_history, default=float('-inf')) 
                if self.can_site_serve_read(site, txn_obj.get_name(), var_idx):
                    if site.getDataManager().check_commit_btw_time_range(last_recovery_time, txn_obj.get_arrival_time(), var_idx):
                        self.process_read_success(site, txn_obj, var_name, var_idx)
                        log.info("Transaction %s has read value %s from Site %s", txn_obj.get_ID(), site.getDataManager().findRecentSnapshot(txn_obj.get_arrival_time(), var_idx))
                        return
                    
                log.debug("Site %s has recovered but no valid write for variable %s.", site.get_id(), var_name)
            elif site.getSiteStatus() == SiteStatus.FAILED:
                #Track sites to wait for if no valid read is possible
                sites_to_wait.append(site)

        #If no site could serve the read, handle failure or wait
        if sites_to_wait:
            log.info("Transaction %s waiting for variable %s due to site failures.", txn_obj.get_name(), var_name)
            self.add_pending_reads(sites_to_wait, txn_obj, var_idx)
        else:
            log.error("Transaction %s failed to read variable %s. No valid sites available.", txn_obj.get_name(), var_name)
            self.process_read_failure(txn_obj, var_name)

    def print_serialization_graph(self):
        """
        Prints the current serialization graph, showing:
        - Nodes (transactions).
        - Outgoing edges (dependencies) with their types.
        """
        if not self.serialization_graph:
            log.info("Serialization graph is empty.")
            return

        log.info("Serialization Graph:")
        for node, edges in self.serialization_graph.items():
            log.info(f"T{node} -> {[(f'T{neighbor}', edge_type) for neighbor, edge_type in edges]}")

    def can_site_serve_read(self, site, txn_name, var_index):
        """Check if a site can service a read request"""
        txn_obj = self.txn_map[txn_name]
        txn_start_time = txn_obj.get_arrival_time() #Get the transaction start time to search for the most recent snapshot of a variable
        data_manager = site.getDataManager()
        site_status = site.getSiteStatus()

        log.debug("Checking if site %s can serve read for transaction %s, variable x%s", site.get_id(), txn_name, var_index)

        #Check if the site is up
        if site_status == SiteStatus.UP:
            """First check if there were any commits to the variable"""
            #recent_snapshot
            recent_snapshot = data_manager.findRecentSnapshot(txn_start_time, var_index)
            if recent_snapshot:
                log.debug("Site %s is UP. Found a valid snapshot for variable x%s", site.get_id(), var_index)
                return True
            else:
                # if recent_snapshot== None and data_manager.findRecentSnapshot(txn_start_time, var_index)
                # log.warning("Site %s is UP but has no valid snapshot for variable x%s", site.get_id(), var_index)
                log.debug("Site %s is UP. Found a valid snapshot for variable x%s", site.get_id(), var_index)
                return True
        
        if site_status == SiteStatus.RECOVERED:
            recovery_history = self.site_manager.site_recover_history.get(site.get_id(), []) 
            last_recovery_time = max(recovery_history, default=float('-inf'))       
            committed_snapshot_time=None 
            # committed_snapshot_time = data_manager.most_recent_snapshot_time()
            recent_snapshot = data_manager.findRecentSnapshot(txn_start_time, var_index)
            for variable in data_manager.getVariableList():
                if variable.getVariableName()[1:]==str(var_index):
                    # recent_snapshot = data_manager.findRecentSnapshot(txn_start_time, var_index)
                    committed_snapshot_time=variable.most_recent_snapshot_time()
                    if committed_snapshot_time:
                        if last_recovery_time < committed_snapshot_time  and committed_snapshot_time < txn_start_time:
                            return True
                    else:
                        return True

        #Otherwise site has failed
        log.error("Site %s cannot serve read for transaction %s, variable x%s. Site is %s", site.get_id(), txn_name, var_index, site_status)
        return False

    def process_read_success(self, site, txn_obj, variable_name, var_index):
        """
        Adds transaction object to access history
        Adds site to site accessed
        """
        log.info("Transaction %s successfully read variable %s from site %s", txn_obj.get_id(), variable_name, site.get_id())

        self.txn_access_hist[txn_obj.get_id()][var_index].append("R")
        txn_obj.add_site_accessed(site.get_id())

    def process_read_failure(self, txn_obj, var_name):
        """Handles a failed read request and marks the transaction in failed state accordingly"""
        log.error("Transaction %s failed to read variable %s: No available sites or valid snapshots", txn_obj.get_id(), var_name)
        txn_obj.set_status(TransactionStatus.ABORTED)

    def add_pending_reads(self, sites, txn_obj, var_index):
        """Adds a read request to the wait list to let the site manager know about the transaction object"""
        for site in sites:
            if self.is_even_index(var_index):
                self.site_manager.add_waitlist_txn_even(site.get_id(), txn_obj, var_index)
            else:
                self.site_manager.add_waitlist_txn_odd(site.get_id(), txn_obj, var_index)
        txn_obj.set_status(TransactionStatus.WAITING)

    def add_node(self,u):
        """Adds node in serialization graph"""
        if u not in self.serialization_graph:
            self.serialization_graph[u] = set()

    def is_even_index(self, variable_index):
        """
        Checks if the variable is even and serves as a helper function to direct the variable to the appropriate site
        """
        if(variable_index % 2 == 0):
            return True
        else:  
            return False
    
    #TO DO: Example: "W(T1, x6,v) says transaction 1 wishes to write all available copies of x6 with the value v. So, T1 can write to x6 on all sites that are up and that contain x6"
    def attempt_write(self, txn_obj, var_idx, value):
        written_flag=False
        """Attempts to perform a update local copy at appropriate sites"""
        if self.is_even_index(var_idx):
            written_flag = False
            for site in self.site_manager.getAllSites():
                if site.getSiteStatus() == SiteStatus.UP:            
                    if self.perform_write_at_up_site(site, var_idx, value, txn_obj):
                        txn_obj.add_site_accessed(site.get_id()) #add to list of sites accessed
                        written_flag = True #Atleast 1 site got written to we return True, else will return False
            return written_flag            
        #                
        else:
            site = self.site_manager.getSite(var_idx % 10)
            if site.getSiteStatus() == SiteStatus.UP:            
                self.perform_write_at_up_site(site, var_idx, value, txn_obj)
                txn_obj.add_site_accessed(site.get_id()) #add to list of sites accessed
                written_flag = True
            return written_flag

    
    def perform_write_at_up_site(self, site, var_idx, value, txn_obj):
        """
            Handles update local copys at a recovered site
            The site is fully operational and does not require additional validation like recovery history or snapshot checking
        """
        data_manager = site.getDataManager()
        if data_manager.update_local_copy(var_idx, value, txn_obj):
            log.info(f"Write succeeded for variable x{var_idx} with value {value} at site {site.get_id()}")
            return True
        else:
            log.warning(f"Write failed for variable x{var_idx} at site {site.get_id()}")
            return False

    def perform_write_at_recovered_site(self, site, var_idx, value, txn_obj):
        """
            Retrieves the most recent snapshot of the variable and validates its commit time against the recovery history
        """
        recovery_history = self.site_manager.get_site_recover_history(site.get_id())
        last_recovery_time = max(recovery_history, default=float('-inf'))

        recent_snapshot = site.getDataManager().findRecentSnapshot(txn_obj.get_arrival_time(), var_idx)
        if recent_snapshot and recent_snapshot.getCommitTime() > last_recovery_time:
            if site.getDataManager().update_local_copy(var_idx, value, txn_obj):
                log.info(f"Write succeeded for variable x{var_idx} at recovered site {site.get_id()}")
                return True
        log.warning(f"Write failed for variable x{var_idx} at recovered site {site.get_id()}")
        return False

    def abort_transaction(self, txn_name, current_time):
        """
        Aborts a transaction and cleans up associated resources. This fx should:
        - abort if there are two rw edges in conflict causing cycle
        - cleans up tentative writes across all sites
        - Updates serialization graph to remove the transaction
        """
        txn_obj = self.txn_map.get(txn_name)
        if not txn_obj:
            log.error(f"Transaction {txn_name} does not exist")
            return

        txn_id = txn_obj.get_id()
        log.info(f"Aborting transaction {txn_name} at time {current_time}")

        #Mark the transaction as aborted
        txn_obj.set_status(TransactionStatus.ABORTED)

        #Cleanup tentative writes at all sites
        for site in self.site_manager.getAllSites():
            # if site.getSiteStatus() == SiteStatus.UP:
            if True:
                data_manager = site.getDataManager()
                data_manager.abort_transaction(txn_obj)
                log.debug(f"Transaction {txn_name} aborted writes at site {site.get_id()}")

        self.retry_pending_transactions()

    def commit_transaction(self, txn_obj, current_time):
        """Commits the transaction by updating all relevant sites"""
        print("inside commit")
        transaction_time = txn_obj.get_arrival_time()
        txn_id = txn_obj.get_id()
        if txn_id in self.txn_access_hist:
            print("inside access_hist of ", {txn_id})
            inner_dict = self.txn_access_hist[txn_id]
            print(inner_dict)
            for var in inner_dict.keys():
                var_name = f"x{var}"
                for elem in inner_dict[var]:
                    if elem=="W":
                        print("inside W")
                        if var%2==0:
                            """If the transaction was writing to an even indexed variable"""
                            for site in self.site_manager.getAllSites():
                                if site.getSiteStatus() == SiteStatus.UP:
                                    data_manager=site.getDataManager()
                                    if data_manager.commit_variable(var_name, current_time, txn_obj):
                                        log.info(f"Variable {var_name} committed at site {site.get_id()} by transaction {txn_obj.get_name()} at time {current_time}.")
                                    else:
                                        log.error(f"Failed to commit variable {var_name} at site {site.get_id()}.")
                                        
                                    for variable in data_manager.getPreCommittedVariablesList():
                                        if variable.getVariableName()[1:]==str(var):
                                            if variable.getCommitTime() > transaction_time:
                                                variable.setCommitTime(current_time)
                                                variable.update_snapshot(current_time,variable.getVariableValue())
                                                # data_manager.updateVariableValue(variable.getVariableName(),variable.getVariableValue())
                                                log.info(f"Variable {variable.getVariableName()} committed at site {site.get_id()} by transaction {txn_obj.get_name()}")
                                
                        else:
                            """If the transaction was writing to an odd indexed variable, it is present only at one site"""
                            for site in self.site_manager.getAllSites():
                                if site.get_id()-1%10==var:
                                    if site.getSiteStatus() == SiteStatus.UP:
                                        data_manager=site.getDataManager()
                                        if data_manager.commit_variable(var_name, current_time, txn_obj):
                                            log.info(f"Variable {var_name} committed at site {site.get_id()} by transaction {txn_obj.get_name()} at time {current_time}.")
                                        else:
                                            log.error(f"Failed to commit variable {var_name} at site {site.get_id()}.")
                                        for variable in data_manager.getPreCommittedVariablesList():
                                            if variable.getVariableName()[1:]==str(var):
                                                if variable.getCommitTime() > transaction_time:
                                                    variable.setCommitTime(current_time)
                                                    variable.update_snapshot(current_time,variable.getVariableValue())
                                                    log.info(f"Variable {variable.getVariableName()} committed at site {site.get_id()} by transaction {txn_obj.get_name()}")


    
    def handle_site_recovery(self, site_id,current_time):
        """
        Manages behavior when a site recovers, potentially allowing blocked transactions to proceed
        """
        log.info(f"Recovering site {site_id}.")
        self.site_manager.recoverSite(site_id)
        self.site_manager.addRecoveredSiteToList(site_id,current_time)
        log.info(f"Site {site_id} recovered successfully")

        #retry transactions waiting on the recovered site
        self.retry_pending_transactions()

    def handle_site_failure(self, site_id):
        """
        Manages behavior when a site fails, updating transaction states as necessary
         - Checks if the txn can access the site
         - aborts write transactions that accessed the failed site
         - does check to see if read transaction can continue to different site
        """
        log.info(f"Handling failure of site {site_id}.")
        self.site_manager.failSite(site_id)  # Mark the site as failed
        log.info(f"Site {site_id} marked as FAILED.")

        #Get the list of all active transactions
        for txn_name, txn_obj in list(self.txn_map.items()):
            txn_id = txn_obj.get_id()
            site_id_idx = int(site_id)
            #check to see if the txn accessed the site
            if site_id_idx in txn_obj.get_sites_accessed():
                if txn_obj.get_transaction_type() == TransactionType.WRITE:
                    #abort write transactions that accessed the failed site
                    log.info(f"Aborting write transaction {txn_name} due to site failure.")
                    self.abort_transaction(txn_name, self.current_time)
                elif txn_obj.get_transaction_type() == TransactionType.READ:
                    #check if read transactions can continue to another available site
                    variables_accessed = self.txn_access_hist[txn_id]
                    can_continue = False
                    for var_idx in variables_accessed:
                        # Check if another site can serve the variable
                        target_site_id = var_idx % 10 + 1
                        if target_site_id != site_id:
                            site = self.site_manager.getSite(target_site_id)
                            if site.getSiteStatus() == SiteStatus.UP:
                                can_continue = True
                                break

                    if not can_continue:
                        log.info(f"Aborting read transaction {txn_name} as it cannot proceed.")
                        self.abort_transaction(txn_name, self.current_time)
                    else:
                        log.info(f"Transaction {txn_name} can proceed using other available sites.")

    def retry_pending_transactions(self):
        """
        Retries pending transactions in the waitlists using existing methods for read/write operations.
        """
        log.info("Retrying pending transactions in the waitlists.")

        #Iterate over all sites managed by the SiteManager
        for site in self.site_manager.getAllSites():
            site_id = site.id
            site_status = site.getSiteStatus()

            #skip sites that are still unavailable
            if site_status == SiteStatus.FAILED:
                log.debug(f"Skipping failed site {site_id}.")
                continue

            log.debug(f"Processing transactions waiting on site {site_id} with status {site_status}.")

            #handle transactions waiting for even-indexed variables
            for txn_obj, var_index in self.site_manager.waitingEvenTxn.get(site_id, []):
                if site_status == SiteStatus.UP or (
                    site_status == SiteStatus.RECOVERED and
                    self.can_site_serve_read(site, txn_obj.get_name(), var_index)
                ):
                    log.info(f"Reattempting transaction {txn_obj.get_id()} for even-indexed variable x{var_index}.")
                    if self.handle_even_indexed_variable(txn_obj, f"x{var_index}", var_index, self.current_time):
                        self.site_manager.waitingEvenTxn[site_id].remove((txn_obj, var_index))
                        log.info(f"Transaction {txn_obj.get_id()} resumed successfully for x{var_index}.")
                    else:
                        log.warning(f"Transaction {txn_obj.get_id()} failed to resume for x{var_index}.")

            #handle transactions waiting for odd-indexed variables
            for txn_obj, var_index in self.site_manager.waitingOddTxn.get(site_id, []):
                if site_status == SiteStatus.UP or (
                    site_status == SiteStatus.RECOVERED and
                    self.can_site_serve_read(site, txn_obj.get_name(), var_index)
                ):
                    log.info(f"Reattempting transaction {txn_obj.get_id()} for odd-indexed variable x{var_index}.")
                    if self.handle_odd_indexed_variable(txn_obj, f"x{var_index}", var_index, self.current_time):
                        self.site_manager.waitingOddTxn[site_id].remove((txn_obj, var_index))
                        log.info(f"Transaction {txn_obj.get_id()} resumed successfully for x{var_index}.")
                    else:
                        log.warning(f"Transaction {txn_obj.get_id()} failed to resume for x{var_index}.")

        log.info("Finished retrying pending transactions.")
