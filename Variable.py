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

class Variable:
    def __init__(self,name,site_idx,val, commit_time=None):
        self.name=name
        self.site_id=site_idx
        self.value=val
        self.commit_time = commit_time
        self.snapshots = []
        self.snapshots.append((0, self.value))

    def getVariable(self):
        return self.value, self.name

    def getVariableValue(self):
        return self.value

    def getVariableName(self):
        return self.name
    
    def getVariableID(self):
        return int(self.name[1:])
    
    def getCommitTime(self):
        return self.commit_time
    
    def setCommitTime(self, commit_time):
        self.commit_time = commit_time

    def setVariableValue(self,value):
        self.value=value

    def update_snapshot(self, timestamp, new_value):
        """
        Update the snapshot
        """
        self.snapshots.append((timestamp, new_value))

    def most_recent_snapshot_time(self):
        """
        Return the timestamp of the most recent snapshot of the variable
        """
        if len(self.snapshots) > 0 :
            return self.snapshots[-1][0]

        return float('-inf')
    
    def find_snapshot_before_time(self, timestamp):
        """
        Return the most recent snapshot of the variable before the specified timestamp
        """
        for i in range(len(self.snapshots)-1, -1, -1):
            entry = self.snapshots[i]
            if entry[0] < timestamp:
                return entry[1]

        return None

    def find_time_of_snapshot_before(self, timestamp):
        """
        Return the time of most recent snapshot of the variable before the specified timestamp
        """
        for i in range(len(self.snapshots)-1, -1, -1):
            entry = self.snapshots[i]
            if entry[0] < timestamp:
                return entry[0]

        return None

    def get_snapshots_list(self):
        """
        Returns the list of snapshots of this variable
        """
        return self.snapshots
