#!/usr/bin/env python3
from dataclasses import dataclass, field
from typing import List, Optional, Dict
import time
from datetime import datetime
import threading
import subprocess
import logging
import re
import os
from enum import Enum
import sqlite3

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Event types
class EventType(Enum):
    FORCE_START = "FORCE_START"
    KILLJOB = "KILLJOB"
    JOB_ON_HOLD = "JOB_ON_HOLD"
    JOB_OFF_HOLD = "JOB_OFF_HOLD"

@dataclass
class Event:
    event_type: EventType
    job_name: str
    timestamp: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d%H%M%S"))

# Core job structure aligned with AutoSys attributes
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
    stderr_file: str = ""        # Standard error file
    stdout: str = ""             # Normal output content
    stderr: str = ""             # Error output content
    contained_jobs: List[str] = None  # For box jobs: list of job names inside

    def __post_init__(self):
        if self.contained_jobs is None:
            self.contained_jobs = []

# SQLite database setup
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
                job.stdout_file, job.stderr_file, job.stdout, job.stderr, ','.join(job.contained_jobs) if job.contained_jobs else ""
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
                if contained_jobs == [""]:  # Handle empty string case
                    contained_jobs = []
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

# Autosys-like scheduler class
class AutosysScheduler:
    def __init__(self, db_path="autosys_jobs.db"):
        self.db = AutosysDB(db_path)
        self.jobs: Dict[str, AutosysJob] = {}
        self.running_threads: Dict[str, threading.Thread] = {}
        self.processes: Dict[str, subprocess.Popen] = {}
        self.events: List[Event] = []
        self.lock = threading.Lock()
        self.load_from_db()

    def load_from_db(self):
        """Load jobs from the database on startup."""
        loaded_jobs = self.db.load_jobs()
        with self.lock:
            for job in loaded_jobs:
                self.jobs[job.insert_job] = job
        logging.info(f"Loaded {len(loaded_jobs)} jobs from database")

    def parse_jil(self, jil_string: str):
        """Parse JIL-like syntax and add jobs to the scheduler and database."""
        current_job = None
        
        for line in jil_string.split('\n'):
            line = line.strip()
            if not line or line.startswith('/*') or line.endswith('*/'):
                continue
                
            # Check for new job definition
            if line.startswith('insert_job:'):
                # Save previous job if exists
                if current_job:
                    self.add_job(current_job)
                    self.db.save_job(current_job)
                    logging.info(f"Added job: {current_job.insert_job}")
                
                # Start new job
                job_name = line.split(':', 1)[1].strip()
                current_job = AutosysJob(insert_job=job_name)
            elif current_job:
                # Parse job attributes
                match = re.match(r'(\w+):\s*(.+)', line)
                if match:
                    key, value = match.groups()
                    value = value.strip('"')
                    
                    if key == 'job_type':
                        current_job.job_type = value
                    elif key == 'command':
                        current_job.command = value
                    elif key == 'machine':
                        current_job.machine = value
                    elif key == 'start_times':
                        current_job.start_times = value
                    elif key == 'box_name':
                        current_job.box_name = value
                    elif key == 'condition':
                        current_job.condition = value
                    elif key == 'description':
                        current_job.description = value
                    elif key == 'max_run_alarm':
                        current_job.max_run_alarm = int(value)
                    elif key == 'owner':
                        current_job.owner = value
                    elif key == 'std_out_file':
                        current_job.stdout_file = value
                    elif key == 'std_err_file':
                        current_job.stderr_file = value
                    elif key == 'stdout':
                        current_job.stdout_file = value
                    elif key == 'stderr':
                        current_job.stderr_file = value
        
        # Save the last job if exists
        if current_job:
            self.add_job(current_job)
            self.db.save_job(current_job)
            logging.info(f"Added job: {current_job.insert_job}")

    def add_job(self, job: AutosysJob):
        """Add a job to the scheduler."""
        with self.lock:
            self.jobs[job.insert_job] = job
            if job.box_name and job.box_name in self.jobs:
                box_job = self.jobs[job.box_name]
                if job.insert_job not in box_job.contained_jobs:
                    box_job.contained_jobs.append(job.insert_job)
                    self.db.save_job(box_job)  # Update box job in DB

    def evaluate_condition(self, condition: str) -> bool:
        """Evaluate AutoSys condition syntax."""
        if not condition:
            return True
            
        with self.lock:
            # Handle multiple conditions connected by AND
            if ' AND ' in condition:
                conditions = condition.split(' AND ')
                return all(self.evaluate_condition(cond.strip()) for cond in conditions)
            
            # Handle success condition s(job_name)
            if condition.startswith('s(') and condition.endswith(')'):
                dep_job_name = condition[2:-1]
                if dep_job_name not in self.jobs:
                    logging.warning(f"Dependency job not found: {dep_job_name}")
                    return False
                
                dep_job = self.jobs[dep_job_name]
                return dep_job.status == "SU"
            
            # Add more condition types as needed
            logging.warning(f"Unsupported condition syntax: {condition}")
            return False

    def evaluate_box_status(self, box_job: AutosysJob) -> bool:
        """Check if all jobs in a box have succeeded."""
        with self.lock:
            if not box_job.contained_jobs:
                return True  # Empty box is considered successful
            return all(self.jobs[job_name].status == "SU" for job_name in box_job.contained_jobs 
                      if job_name in self.jobs)

    def check_dependent_jobs(self, job: AutosysJob):
        """Check for dependent jobs that can now run after this job completes."""
        if not job.box_name:
            return
            
        box_job = self.jobs.get(job.box_name)
        if not box_job:
            return
            
        logging.info(f"Checking dependencies after completion of {job.insert_job}")
        
        for dep_job_name in box_job.contained_jobs:
            dep_job = self.jobs.get(dep_job_name)
            if not dep_job or dep_job.insert_job == job.insert_job:
                continue
                
            if dep_job.status == "IN" and dep_job.condition:
                # Check if this job depends on the completed job
                if f"s({job.insert_job})" in dep_job.condition:
                    logging.info(f"Job {dep_job.insert_job} depends on {job.insert_job}")
                    
                    # If the condition is now satisfied, start the dependent job
                    if self.evaluate_condition(dep_job.condition):
                        logging.info(f"Starting dependent job {dep_job.insert_job} after {job.insert_job} completed")
                        thread = threading.Thread(target=self.run_job, args=(dep_job,))
                        thread.start()
                        with self.lock:
                            self.running_threads[dep_job.insert_job] = thread

    def run_command_job(self, job: AutosysJob):
        """Execute a command job."""
        try:
            job.status = "RU"
            job.last_start = datetime.now().strftime("%Y%m%d%H%M%S")
            logging.info(f"Starting job: {job.insert_job} at {job.last_start}")
            self.db.save_job(job)

            # Set up output files or use default stdout/stderr
            stdout_dest = None
            stderr_dest = None
            
            if job.stdout_file:
                # Ensure directory exists
                os.makedirs(os.path.dirname(job.stdout_file) if os.path.dirname(job.stdout_file) else '.', exist_ok=True)
                stdout_dest = open(job.stdout_file, 'w')
            
            if job.stderr_file:
                # Ensure directory exists
                os.makedirs(os.path.dirname(job.stderr_file) if os.path.dirname(job.stderr_file) else '.', exist_ok=True)
                stderr_dest = open(job.stderr_file, 'w')
            
            # Start the process with appropriate output destinations
            process = subprocess.Popen(
                job.command, 
                shell=True, 
                stdout=stdout_dest or subprocess.PIPE, 
                stderr=stderr_dest or subprocess.PIPE
            )
            
            with self.lock:
                self.processes[job.insert_job] = process

            # If using PIPE (not file), capture the output
            if not stdout_dest or not stderr_dest:
                stdout_data, stderr_data = process.communicate(timeout=job.max_run_alarm * 60 if job.max_run_alarm else None)
                
                # Store outputs for database
                if not stdout_dest:
                    job.stdout = stdout_data.decode() if stdout_data else ""
                    # Print to screen if no file specified
                    print(f"--- {job.insert_job} STDOUT ---\n{job.stdout}")
                    
                if not stderr_dest:
                    job.stderr = stderr_data.decode() if stderr_data else ""
                    # Print to screen if no file specified
                    print(f"--- {job.insert_job} STDERR ---\n{job.stderr}")
            else:
                # Wait for process to complete when using files
                process.wait(timeout=job.max_run_alarm * 60 if job.max_run_alarm else None)
            
            # Close file handles if used
            if stdout_dest and not stdout_dest.closed:
                stdout_dest.close()
            if stderr_dest and not stderr_dest.closed:
                stderr_dest.close()
                
            job.last_end = datetime.now().strftime("%Y%m%d%H%M%S")

            with self.lock:
                if job.insert_job in self.processes:
                    del self.processes[job.insert_job]

            if process.returncode == 0:
                job.status = "SU"
                logging.info(f"Job {job.insert_job} completed successfully")
            else:
                job.status = "FA"
                logging.error(f"Job {job.insert_job} failed with exit code {process.returncode}")
                
            self.db.save_job(job)
            
            # Check if this job is part of a box, if so, update box status
            if job.box_name and job.box_name in self.jobs:
                self.update_box_status(self.jobs[job.box_name])
                
            # Check if any dependent jobs should now run - crucial for dependency chains
            self.check_dependent_jobs(job)
                
        except subprocess.TimeoutExpired:
            if process:
                process.kill()
            job.status = "FA"
            job.stderr = "Job exceeded max_run_alarm"
            logging.error(f"Job {job.insert_job} timed out")
            self.db.save_job(job)
            
            # Close file handles if used
            if stdout_dest and not stdout_dest.closed:
                stdout_dest.close()
            if stderr_dest and not stderr_dest.closed:
                stderr_dest.close()
        except Exception as e:
            job.status = "FA"
            job.stderr = str(e)
            logging.error(f"Job {job.insert_job} failed with exception: {e}")
            self.db.save_job(job)
            
            # Close file handles if used
            if stdout_dest and not stdout_dest.closed:
                stdout_dest.close()
            if stderr_dest and not stderr_dest.closed:
                stderr_dest.close()

    def update_box_status(self, box_job: AutosysJob):
        """Update box job status based on contained jobs."""
        if box_job.status != "RU":
            return
            
        all_jobs_completed = True
        all_jobs_succeeded = True
        
        for job_name in box_job.contained_jobs:
            job = self.jobs.get(job_name)
            if not job:
                continue
                
            if job.status not in ["SU", "FA"]:
                all_jobs_completed = False
                break
                
            if job.status == "FA":
                all_jobs_succeeded = False
        
        if all_jobs_completed:
            box_job.status = "SU" if all_jobs_succeeded else "FA"
            box_job.last_end = datetime.now().strftime("%Y%m%d%H%M%S")
            logging.info(f"Box job {box_job.insert_job} completed with status {box_job.status}")
            self.db.save_job(box_job)

    def run_box_job(self, box_job: AutosysJob):
        """Handle box job execution by triggering contained jobs."""
        if not box_job.contained_jobs:
            logging.info(f"Box job {box_job.insert_job} has no contained jobs")
            box_job.status = "SU"  # Empty box is successful
            box_job.last_start = datetime.now().strftime("%Y%m%d%H%M%S")
            box_job.last_end = box_job.last_start
            self.db.save_job(box_job)
            return
            
        box_job.status = "RU"
        box_job.last_start = datetime.now().strftime("%Y%m%d%H%M%S")
        logging.info(f"Starting box job: {box_job.insert_job} with {len(box_job.contained_jobs)} contained jobs")
        self.db.save_job(box_job)
        
        # Start jobs that have no dependencies or dependencies that are satisfied
        jobs_started = False
        for job_name in box_job.contained_jobs:
            job = self.jobs.get(job_name)
            if not job:
                continue
                
            if job.status == "IN" and self.evaluate_condition(job.condition):
                thread = threading.Thread(target=self.run_job, args=(job,))
                thread.start()
                with self.lock:
                    self.running_threads[job_name] = thread
                jobs_started = True
                logging.info(f"Started job {job_name} from box {box_job.insert_job}")
        
        if not jobs_started:
            logging.warning(f"No jobs in box {box_job.insert_job} were eligible to start")

    def should_run(self, job: AutosysJob, force: bool = False) -> bool:
        """Determine if a job should run."""
        if job.status == "OH" and not force:
            return False
            
        # Box jobs with no start time should still run for dependencies
        if job.job_type == "b" and not job.start_times and not force:
            return False
            
        # Command jobs without start time don't run unless forced
        if job.job_type == "c" and not job.start_times and not force:
            return False
            
        # For jobs with a start time, check if it's time to run
        if job.start_times:
            current_time = datetime.now().strftime("%H:%M")
            if not force and current_time < job.start_times:
                return False
        
        # Don't run if conditions aren't met, unless forced
        if not force and not self.evaluate_condition(job.condition):
            return False
            
        # Only run jobs with inactive status or being forced
        return force or job.status == "IN"

    def process_event(self, event: Event):
        """Handle AutoSys-like events."""
        with self.lock:
            job = self.jobs.get(event.job_name)
            if not job:
                logging.error(f"Unknown job: {event.job_name}")
                return

            if event.event_type == EventType.FORCE_START:
                if job.status in ["IN", "OH"]:
                    thread = threading.Thread(target=self.run_job, args=(job, True))
                    thread.start()
                    self.running_threads[job.insert_job] = thread
                    logging.info(f"Force starting job: {job.insert_job}")
            elif event.event_type == EventType.KILLJOB:
                if job.status == "RU" and job.insert_job in self.processes:
                    self.processes[job.insert_job].kill()
                    job.status = "FA"
                    job.stderr = "Job killed by event"
                    logging.info(f"Killed job: {job.insert_job}")
                    self.db.save_job(job)
            elif event.event_type == EventType.JOB_ON_HOLD:
                job.status = "OH"
                logging.info(f"Job {job.insert_job} put on hold")
                self.db.save_job(job)
            elif event.event_type == EventType.JOB_OFF_HOLD:
                if job.status == "OH":
                    job.status = "IN"
                    logging.info(f"Job {job.insert_job} taken off hold")
                    self.db.save_job(job)

    def run_job(self, job: AutosysJob, force: bool = False):
        """Run a job based on its type."""
        if job.job_type == "c":
            self.run_command_job(job)
        elif job.job_type == "b":
            self.run_box_job(job)

    def schedule(self):
        """Main scheduling loop."""
        while True:
            with self.lock:
                # Process events
                for event in list(self.events):  # Use a copy of the list
                    self.process_event(event)
                    self.events.remove(event)

                # Check for jobs that need to be run based on dependencies
                # First scan for all running boxes
                running_boxes = [job for job in self.jobs.values() 
                                if job.job_type == "b" and job.status == "RU"]
                
                # For each running box, check for jobs that can now be started
                for box_job in running_boxes:
                    for job_name in box_job.contained_jobs:
                        job = self.jobs.get(job_name)
                        if not job:
                            continue
                            
                        if job.status == "IN" and self.evaluate_condition(job.condition):
                            # Job's dependencies are satisfied, so start it
                            thread = threading.Thread(target=self.run_job, args=(job,))
                            thread.start()
                            self.running_threads[job_name] = thread
                            logging.info(f"Starting job {job_name} due to satisfied dependencies")
                
                # Check for standalone jobs or box jobs to start based on start_times
                current_time = datetime.now().strftime("%H:%M")
                for job_name, job in self.jobs.items():
                    if job.status == "IN" and job.start_times and current_time >= job.start_times:
                        if self.should_run(job):
                            thread = threading.Thread(target=self.run_job, args=(job,))
                            thread.start()
                            self.running_threads[job_name] = thread

            # Clean up completed threads
            with self.lock:
                completed_threads = []
                for job_name, thread in self.running_threads.items():
                    if not thread.is_alive():
                        completed_threads.append(job_name)
                        
                for job_name in completed_threads:
                    del self.running_threads[job_name]
                
            time.sleep(10)  # Check every 10 seconds

    def start(self):
        """Start the scheduler."""
        scheduler_thread = threading.Thread(target=self.schedule, daemon=True)
        scheduler_thread.start()
        logging.info("Scheduler started")

    def send_event(self, event: Event):
        """Send an event to the scheduler."""
        with self.lock:
            self.events.append(event)

    def get_job_status(self, job_name: str) -> Optional[AutosysJob]:
        """Get job status."""
        with self.lock:
            return self.jobs.get(job_name)

# Example usage
def main():
    scheduler = AutosysScheduler()
    
    # Parse JIL definitions
    jil_definitions = """
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
    
    # Parse the JIL definitions
    scheduler.parse_jil(jil_definitions)
    
    # Start the scheduler
    scheduler.start()
    
    # Wait a moment for scheduler to initialize
    time.sleep(2)
    
    # Force start the box job
    scheduler.send_event(Event(EventType.FORCE_START, "Box1"))

    # Monitor status
    while True:
        for job_name in ["Box1", "Job1", "Job2"]:
            job = scheduler.get_job_status(job_name)
            if job:
                print(f"{job.insert_job}: Status={job.status}, LastStart={job.last_start}, LastEnd={job.last_end}")
        time.sleep(10)

if __name__ == "__main__":
    main()
