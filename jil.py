#!/usr/bin/env python3
import sqlite3
import re
import os
from dataclasses import dataclass, field
from typing import List, Dict
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Core job structure aligned with AutoSys attributes

def black(text):
    return f'\033[30m{text}\033[0m'

def red(text):
    return f'\033[31m{text}\033[0m'

def green(text):
    return f'\033[32m{text}\033[0m'

def yellow(text):
    return f'\033[33m{text}\033[0m'

def blue(text):
    return f'\033[34m{text}\033[0m'

def magenta(text):
    return f'\033[35m{text}\033[0m'

def cyan(text):
    return f'\033[36m{text}\033[0m'

def gray(text):
    return f'\033[90m{text}\033[0m'

@dataclass
class AutosysJob:
    insert_job: str              # Job name
    job_type: str = "c"          # 'c' (command), 'b' (box), 'f' (file watcher)
    command: str = ""            # Command to execute (for command jobs)
    machine: str = "localhost"   # Machine to run on
    start_times: str = ""        # Start time in "HH:MM" format
    box_name: str = ""           # Parent box name
    condition: str = ""          # Dependency condition
    status: str = "IN"           # IN, SU, RU, FA, OH (On Hold)
    description: str = ""        # Job description
    max_run_alarm: int = 0       # Max runtime in minutes
    owner: str = "user"          # Job owner
    last_start: str = ""         # Last start time
    last_end: str = ""           # Last end time
    stdout_file: str = ""        # Standard output file
    stderr_file: str = ""        # Standard error output file
    stdout: str = ""             # Normal output content
    stderr: str = ""             # Error output content
    contained_jobs: List[str] = None  # For box jobs: list of job names inside

    def __post_init__(self):
        if self.contained_jobs is None:
            self.contained_jobs = []

# SQLite database class
class AutosysDB:
    def __init__(self, db_path="autosys_jobs.db"):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        """Initialize the SQLite database and create the jobs table."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    insert_job TEXT PRIMARY KEY,
                    job_type TEXT,
                    command TEXT,
                    machine TEXT,
                    start_times TEXT,
                    box_name TEXT,
                    condition TEXT,
                    status TEXT,
                    description TEXT,
                    max_run_alarm INTEGER,
                    owner TEXT,
                    last_start TEXT,
                    last_end TEXT,
                    stdout_file TEXT,
                    stderr_file TEXT,
                    stdout TEXT,
                    stderr TEXT,
                    contained_jobs TEXT
                )
            ''')
            conn.commit()

    def save_job(self, job: AutosysJob):
        """Save or update a job in the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO jobs (
                    insert_job, job_type, command, machine, start_times, box_name, condition,
                    status, description, max_run_alarm, owner, last_start, last_end, 
                    stdout_file, stderr_file, stdout, stderr, contained_jobs
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job.insert_job, job.job_type, job.command, job.machine, job.start_times, job.box_name, job.condition,
                job.status, job.description, job.max_run_alarm, job.owner, job.last_start, job.last_end,
                job.stdout_file, job.stderr_file, job.stdout, job.stderr, ','.join(job.contained_jobs)
            ))
            conn.commit()

    def load_jobs(self) -> List[AutosysJob]:
        """Load all jobs from the database."""
        jobs = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs")
            for row in cursor.fetchall():
                contained_jobs = row['contained_jobs'].split(',') if row['contained_jobs'] else []
                job = AutosysJob(
                    insert_job=row['insert_job'],
                    job_type=row['job_type'],
                    command=row['command'],
                    machine=row['machine'],
                    start_times=row['start_times'],
                    box_name=row['box_name'],
                    condition=row['condition'],
                    status=row['status'],
                    description=row['description'],
                    max_run_alarm=row['max_run_alarm'],
                    owner=row['owner'],
                    last_start=row['last_start'],
                    last_end=row['last_end'],
                    stdout_file=row['stdout_file'],
                    stderr_file=row['stderr_file'],
                    stdout=row['stdout'],
                    stderr=row['stderr'],
                    contained_jobs=contained_jobs
                )
                jobs.append(job)
        return jobs

# JIL Parser class
class JILParser:
    def __init__(self, db_path="autosys_jobs.db"):
        self.db = AutosysDB(db_path)
        self.jobs: Dict[str, AutosysJob] = {}

    def parse_file(self, file_path: str):
        """Parse a JIL file and save jobs to the database."""
        try:
            with open(file_path, 'r') as file:
                jil_content = file.read()
            self.parse_jil(jil_content)
        except FileNotFoundError:
            logging.error(f"JIL file not found: {file_path}")
        except Exception as e:
            logging.error(f"Error parsing JIL file {file_path}: {e}")

    def parse_jil(self, jil_content: str):
        """Parse JIL content and create AutosysJob objects."""
        current_job = {}
        in_job_definition = False

        for line in jil_content.split('\n'):
            line = line.strip()

            # Skip empty lines or full-line comments
            if not line or line.startswith('/*') or line.startswith('*/') or line == '*/':
                continue

            # Check for line with both comment and content
            if '/*' in line and '*/' in line:
                # Extract only the part before the comment
                line = line.split('/*')[0].strip()
                if not line:
                    continue

            # Check for start of a job definition
            if 'insert_job:' in line:
                if in_job_definition and current_job:
                    # Save the previous job if it exists
                    self._save_job(current_job)
                current_job = {}
                in_job_definition = True

            # Parse key-value pairs
            match = re.match(r'(\w+)\s*:\s*(.+)', line)
            if match:
                key, value = match.groups()
                current_job[key] = value.strip('"')

        # Save the last job if it exists
        if in_job_definition and current_job:
            self._save_job(current_job)

        # Update contained_jobs for box jobs after all jobs are parsed
        self._update_box_contained_jobs()

    def _save_job(self, job_dict: Dict[str, str]):
        """Convert job dictionary to AutosysJob and save to DB."""
        if 'insert_job' not in job_dict:
            logging.warning("Job definition missing 'insert_job', skipping")
            return

        autosys_job = AutosysJob(
            insert_job=job_dict.get('insert_job', ''),
            job_type=job_dict.get('job_type', 'c'),
            command=job_dict.get('command', ''),
            machine=job_dict.get('machine', 'localhost'),
            start_times=job_dict.get('start_times', ''),
            box_name=job_dict.get('box_name', ''),
            condition=job_dict.get('condition', ''),
            description=job_dict.get('description', ''),
            max_run_alarm=int(job_dict.get('max_run_alarm', 0)),
            owner=job_dict.get('owner', 'user'),
            stdout_file=job_dict.get('stdout', ''),
            stderr_file=job_dict.get('stderr', '')
        )
        
        self.jobs[autosys_job.insert_job] = autosys_job
        self.db.save_job(autosys_job)
        logging.info(f"Saved job to database: {autosys_job.insert_job}")

    def _update_box_contained_jobs(self):
        """Update contained_jobs for box jobs after parsing all jobs."""
        for job_name, job in self.jobs.items():
            if job.box_name and job.box_name in self.jobs:
                box_job = self.jobs[job.box_name]
                if job.insert_job not in box_job.contained_jobs:
                    box_job.contained_jobs.append(job.insert_job)
                    self.db.save_job(box_job)  # Update the box job in the database
                    logging.info(f"Updated box {box_job.insert_job} with contained job {job.insert_job}")

# Job execution class with stdout/stderr handling
class JobExecutor:
    def __init__(self, db: AutosysDB):
        self.db = db
    
    def execute_job(self, job_name: str):
        """Execute a job and capture stdout/stderr."""
        jobs = self.db.load_jobs()
        job = next((j for j in jobs if j.insert_job == job_name), None)
        
        if not job:
            logging.error(f"Job {job_name} not found")
            return False
            
        if job.job_type != 'c':
            logging.error(f"Can only execute command jobs. {job_name} is type {job.job_type}")
            return False
            
        # Update job status
        job.status = "RU"
        job.last_start = self._get_current_timestamp()
        self.db.save_job(job)
        
        try:
            import subprocess
            import tempfile
            
            # Create temporary files for output capture
            with tempfile.NamedTemporaryFile(delete=False) as out_temp, \
                 tempfile.NamedTemporaryFile(delete=False) as err_temp:
                
                out_path = out_temp.name
                err_path = err_temp.name
            
            # Execute the command
            process = subprocess.Popen(
                job.command, 
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate()
            
            # Save output to database
            job.stdout = stdout
            job.stderr = stderr
            
            # Write to specified output files if configured
            if job.stdout_file:
                self._write_to_file(job.stdout_file, stdout)
                logging.info(f"Wrote stdout to {job.stdout_file}")
                
            if job.stderr_file:
                self._write_to_file(job.stderr_file, stderr)
                logging.info(f"Wrote stderr to {job.stderr_file}")
            
            # Update job status
            #job.status = "SU" if process.returncode == 0 else "FA"
            job.last_end = self._get_current_timestamp()
            self.db.save_job(job)
            
            logging.info(f"Job {job_name} executed with status {job.status}")
            return job.status == "SU"
            
        except Exception as e:
            logging.error(f"Error executing job {job_name}: {e}")
            job.status = "FA"
            job.stderr = str(e)
            job.last_end = self._get_current_timestamp()
            self.db.save_job(job)
            return False
    
    def _write_to_file(self, file_path: str, content: str):
        """Write content to a file, creating directories if needed."""
        try:
            # Create directory if it doesn't exist
            dir_path = os.path.dirname(file_path)
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path)
                
            # Write content to file
            with open(file_path, 'w') as f:
                f.write(content)
                
        except Exception as e:
            logging.error(f"Error writing to file {file_path}: {e}")
    
    def _get_current_timestamp(self):
        """Get current timestamp in a suitable format."""
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Example usage
def main():
    from sys import argv
    # Create a sample JIL file
    sample_jil = """
    /* Definition for Box1 */
    insert_job: Box1
    job_type: b
    start_times: "00:00"
    description: "Container box job"

    /* Definition for Job1 */
    insert_job: Job1
    job_type: c
    command: "ps -ef"
    box_name: Box1
    max_run_alarm: 5
    description: "First job in box"
    stdout: "output/job1_output.log"
    stderr: "errors/job1_errors.log"

    /* Definition for Job2 */
    insert_job: Job2
    job_type: c
    command: "ls -l"
    box_name: Box1
    description: "Second job in box"
    condition: s(Job1)
    max_run_alarm: 5
    stdout: "output/job2_output.log"
    stderr: "errors/job2_errors.log"
    """
    if len(argv) > 1:
        jilfile=argv[1]
    else:
        jilfile="sample.jil"

    # Write sample JIL to a file
    if len(argv) == 1:
      with open("sample.jil", "w") as f:
        f.write(sample_jil)

    # Initialize parser and parse the file
    parser = JILParser()
    parser.parse_file(jilfile)

    # Verify by loading jobs from the database
    db = AutosysDB()
    loaded_jobs = db.load_jobs()
    for job in loaded_jobs:
        print(f"Loaded Job: {job.insert_job}, Type: {job.job_type}, Box: {job.box_name}")
        if job.stdout_file or job.stderr_file:
            print(f"  Output files: stdout={job.stdout_file}, stderr={job.stderr_file}")
        print(f"  Contained Jobs: {job.contained_jobs}")
    
    # Execute the jobs uncomment this if you want them to execute on creation
    # Currently for test purposes

    #executor = JobExecutor(db)
    #print("\nExecuting Job1...")
    #executor.execute_job("Job1")
    
    # Check updated job status and output
    #updated_jobs = db.load_jobs()
    #for job in updated_jobs:
    #    if job.insert_job == "Job1":
    #        print(f"Job1 Status: {job.status}")
    #        print(f"Job1 Output: {job.stdout}")
    #        print(f"Check output files at: {job.stdout_file} and {job.stderr_file}")

if __name__ == "__main__":
    from sys import argv 
    if len(argv) == 1:
        print(red("*************************************************"))
        print (red("no jilfile passed creating  and using sample.jil"))
        print(red("*************************************************\n\n"))
    main()
