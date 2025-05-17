#!/usr/bin/env python3
import sqlite3
import argparse
from typing import List
import logging
from datetime import datetime

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQLite database class (reused from previous implementation)
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
                    stdout TEXT,
                    stderr TEXT,
                    contained_jobs TEXT
                )
            ''')
            conn.commit()

    def load_jobs(self) -> List[dict]:
        """Load all jobs from the database as dictionaries."""
        jobs = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs")
            for row in cursor.fetchall():
                jobs.append(dict(row))
        return jobs

    def load_job(self, job_name: str) -> dict:
        """Load a specific job from the database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs WHERE insert_job = ?", (job_name,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def load_jobs_by_pattern(self, pattern: str) -> List[dict]:
        """Load jobs matching a pattern using SQL LIKE operator."""
        jobs = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs WHERE insert_job LIKE ?", (pattern,))
            for row in cursor.fetchall():
                jobs.append(dict(row))
        return jobs

# Autorep-like reporting class
class Autorep:
    def __init__(self, db_path="autosys_jobs.db"):
        self.db = AutosysDB(db_path)

    def format_time(self, timestamp: str) -> str:
        """Convert timestamp to readable format (e.g., '20230215123000' -> '02/15/2023 12:30:00')."""
        if not timestamp:
            return "N/A"
        try:
            dt = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
            return dt.strftime("%m/%d/%Y %H:%M:%S")
        except ValueError:
            return timestamp  # Return as-is if format is invalid

    def summary_report(self):
        """Generate a summary report similar to 'autorep -s'."""
        jobs = self.db.load_jobs()
        if not jobs:
            print("No jobs found in the database.")
            return

        print(f"{'Job Name':<20} {'Last Start':<20} {'Last End':<20} {'Status':<10} {'Type':<10} {'Box Name':<20}")
        print("-" * 100)
        for job in jobs:
            last_start = self.format_time(job['last_start'])
            last_end = self.format_time(job['last_end'])
            print(f"{job['insert_job']:<20} {last_start:<20} {last_end:<20} {job['status']:<10} {job['job_type']:<10} {job['box_name'] or 'N/A':<20}")

    def job_report(self, job_name: str, detail: bool = False):
        """
        Generate a report for job(s) based on job_name.
        If job_name contains wildcards (% or _), use pattern matching,
        otherwise do an exact match.
        """
        # Check if the job_name contains wildcards
        is_pattern = '%' in job_name or '_' in job_name
        
        if is_pattern:
            # Pattern matching
            jobs = self.db.load_jobs_by_pattern(job_name)
            if not jobs:
                print(f"No jobs matching pattern '{job_name}' found in the database.")
                return
                
            print(f"Found {len(jobs)} jobs matching pattern '{job_name}':")
            
            if detail:
                for job in jobs:
                    self._print_job_details(job)
                    if len(jobs) > 1:  # Add separator only if there are multiple jobs
                        print("\n" + "-" * 80 + "\n")
            else:
                # Summary format for matching jobs
                print(f"{'Job Name':<20} {'Last Start':<20} {'Last End':<20} {'Status':<10} {'Type':<10} {'Box Name':<20}")
                print("-" * 100)
                for job in jobs:
                    last_start = self.format_time(job['last_start'])
                    last_end = self.format_time(job['last_end'])
                    print(f"{job['insert_job']:<20} {last_start:<20} {last_end:<20} {job['status']:<10} {job['job_type']:<10} {job['box_name'] or 'N/A':<20}")
        else:
            # Exact match
            job = self.db.load_job(job_name)
            if not job:
                print(f"Job '{job_name}' not found in the database.")
                return
                
            self._print_job_details(job)

    def _print_job_details(self, job):
        """Helper method to print detailed job information."""
        print(f"\nDetailed Report for Job: {job['insert_job']}")
        print("-" * 50)
        print(f"{'Attribute':<20} {'Value':<30}")
        print("-" * 50)
        print(f"{'Job Name':<20} {job['insert_job']:<30}")
        print(f"{'Job Type':<20} {job['job_type']:<30}")
        print(f"{'Command':<20} {job['command'] or 'N/A':<30}")
        print(f"{'Machine':<20} {job['machine']:<30}")
        print(f"{'Start Times':<20} {job['start_times'] or 'N/A':<30}")
        print(f"{'Box Name':<20} {job['box_name'] or 'N/A':<30}")
        print(f"{'Condition':<20} {job['condition'] or 'N/A':<30}")
        print(f"{'Status':<20} {job['status']:<30}")
        print(f"{'Description':<20} {job['description'] or 'N/A':<30}")
        print(f"{'Max Run Alarm':<20} {job['max_run_alarm'] or 0:<30} minutes")
        print(f"{'Owner':<20} {job['owner']:<30}")
        print(f"{'Last Start':<20} {self.format_time(job['last_start']):<30}")
        print(f"{'Last End':<20} {self.format_time(job['last_end']):<30}")
        print(f"{'Stdout':<20} {job['stdout'] or 'N/A':<30}")
        print(f"{'Stderr':<20} {job['stderr'] or 'N/A':<30}")
        contained_jobs = job['contained_jobs'].split(',') if job['contained_jobs'] else []
        print(f"{'Contained Jobs':<20} {', '.join(contained_jobs) or 'N/A':<30}")

    def run(self, job_name: str = None, summary: bool = False, detail: bool = False):
        """Run the autorep tool based on provided options."""
        if summary:
            self.summary_report()
        elif job_name:
            self.job_report(job_name, detail)
        else:
            print("Please specify an option: -s for summary or -J <job_name> for job report.")

# Command-line interface
def main():
    parser = argparse.ArgumentParser(description="AutoSys-like autorep tool for job reporting.")
    parser.add_argument("-J", "--job", help="Specify a job name or pattern (with % for wildcards)")
    parser.add_argument("-s", "--summary", action="store_true", help="Generate summary report for all jobs")
    parser.add_argument("-d", "--detail", action="store_true", help="Generate detailed report (use with -J)")
    
    args = parser.parse_args()

    autorep = Autorep()
    autorep.run(job_name=args.job, summary=args.summary, detail=args.detail)

if __name__ == "__main__":
    # For demonstration, assume the database is populated from previous JIL parser run
    main()
