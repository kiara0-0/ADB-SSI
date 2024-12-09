import re
import sys
from TransactionManager import TransactionManager
from SiteManager import SiteManager
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

class Simulator:
    def __init__(self):
        self.current_time = 0
        self.num_variables = 20  #Set the number of variables
        self.num_sites = 10  #Set the number of sites
        self.site_manager = SiteManager(self.num_sites)  #Create a SiteManager instance
        self.transaction_manager = TransactionManager(self.num_variables, self.num_sites, self.site_manager)

    def trim(self, string):
        """Utility function to trim whitespace"""
        return string.strip()

    def match_instruction(self, line, pattern):
        """Match an instruction using regex"""
        return re.match(pattern, line)

    def get_instruction_type(self, line):
        """Classify instruction type based on its prefix"""
        if line.startswith("begin("):
            return "BEGIN"
        if line.startswith("R("):
            return "READ"
        if line.startswith("W("):
            return "WRITE"
        if line.startswith("end("):
            return "END"
        if line.startswith("fail("):
            return "FAIL"
        if line.startswith("recover("):
            return "RECOVER"
        if line == "dump()":
            return "DUMP"
        return "UNKNOWN"

    def process_instruction(self, line):
        """Process a single instruction."""
        trimmed_line = self.trim(line)

        #Skip empty lines and comments
        if not trimmed_line or trimmed_line.startswith("/"):
            return

        #Increment current time with each instruction
        self.current_time += 1
        log.debug(f"Processing instruction at time {self.current_time}: {trimmed_line}")

        instruction_type = self.get_instruction_type(trimmed_line)

        if instruction_type == "BEGIN":
            match = self.match_instruction(trimmed_line, r"begin\((\w+)\)")
            if match:
                self.transaction_manager.begin_transaction(match.group(1), self.current_time)

        elif instruction_type == "READ":
            match = self.match_instruction(trimmed_line, r"R\((\w+),\s*(\w+)\)")
            if match:
                self.transaction_manager.read_request(match.group(1), match.group(2), self.current_time)

        elif instruction_type == "WRITE":
            match = self.match_instruction(trimmed_line, r"W\((\w+),\s*(\w+),\s*(\d+)\)")
            if match:
                self.transaction_manager.write_request(match.group(1), match.group(2), int(match.group(3)), self.current_time)

        elif instruction_type == "END":
            match = self.match_instruction(trimmed_line, r"end\((\w+)\)")
            if match:
                self.transaction_manager.end_transaction(match.group(1), self.current_time)

        elif instruction_type == "FAIL": #To fail a site with a specific id
            match = self.match_instruction(trimmed_line, r"fail\((\d+)\)")
            if match:
                log.info(f"Site {match.group(1)} failed")
                self.transaction_manager.handle_site_failure(match.group(1))
        elif instruction_type == "RECOVER": #To recover a site with a specific id
            match = self.match_instruction(trimmed_line, r"recover\((\d+)\)")
            if match:
                log.info(f"Site {match.group(1)} recovered")
                self.transaction_manager.handle_site_recovery(match.group(1),self.current_time)

        elif instruction_type == "DUMP":
            log.info("Executing DUMP command...")
            self.site_manager.dump() 
        else:
            print(f"Unknown instruction: {trimmed_line}")

    def run(self, input_file):
        """Run the simulator by processing instructions from the input file."""
        try:
            with open(input_file, "r") as file:
                for line in file:
                    self.process_instruction(line)
        except FileNotFoundError:
            print(f"Error: File {input_file} not found")
        except Exception as e:
            print(f"An error occurred: {e}")         

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input_file>")
    else:
        simulator = Simulator()
        simulator.run(sys.argv[1])