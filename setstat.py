#!/usr/bin/env python3
import sqlite3
import argparse
import logging
import sys
import re
import os.path

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def set_jobs_status(db_path, job_pattern, status='IN', dry_run=False):
    """
    Set the status of jobs matching the pattern to the specified status.
    
    Args:
        db_path (str): Path to the SQLite database
        job_pattern (str): SQL LIKE pattern to match job names
        status (str): Status to set for the matching jobs (default: 'IN')
        dry_run (bool): If True, only show what would be changed without making changes
    
    Returns:
        int: Number of jobs updated
    """
    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        return 0
        
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # First get matching jobs to show what will be updated
            cursor.execute("SELECT insert_job, status FROM jobs WHERE insert_job LIKE ?", (job_pattern,))
            matching_jobs = cursor.fetchall()
            
            if not matching_jobs:
                logger.warning(f"No jobs found matching pattern: {job_pattern}")
                return 0
                
            logger.info(f"Found {len(matching_jobs)} job(s) matching pattern: {job_pattern}")
            for job in matching_jobs:
                current_status = job['status']
                job_name = job['insert_job']
                action = "Would set" if dry_run else "Setting"
                logger.info(f"{action} job '{job_name}' from status '{current_status}' to '{status}'")
            
            # If not dry run, actually update the database
            if not dry_run:
                cursor.execute("UPDATE jobs SET status = ? WHERE insert_job LIKE ?", (status, job_pattern))
                conn.commit()
                logger.info(f"Successfully updated {len(matching_jobs)} job(s) to '{status}' status")
            else:
                logger.info("Dry run completed. No changes made.")
                
            return len(matching_jobs)
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 0

def is_valid_pattern(pattern):
    """
    Basic validation for SQL LIKE patterns to prevent SQL injection.
    Just makes sure there are no semicolons or other suspicious characters.
    """
    if ";" in pattern:
        return False
    if re.search(r"--|\bOR\b|\bAND\b|\bUNION\b|\bSELECT\b|\bDROP\b|\bDELETE\b|\bUPDATE\b", 
                pattern, re.IGNORECASE):
        return False
    return True

def main():
    parser = argparse.ArgumentParser(description='Set AutoSys jobs status')
    parser.add_argument('--db', '-d', default='autosys_jobs.db', 
                        help='Path to AutoSys SQLite database (default: autosys_jobs.db)')
    parser.add_argument('--pattern', '-p', required=True, 
                        help='SQL LIKE pattern to match job names (e.g. "Job%", "Box_%%")')
    parser.add_argument('--status', '-s', default='IN',
                        help='Status to set for matched jobs (default: IN)')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='Show what would be changed without making actual changes')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set log level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate pattern to prevent SQL injection
    if not is_valid_pattern(args.pattern):
        logger.error(f"Invalid pattern: {args.pattern} (contains disallowed characters)")
        sys.exit(1)
    
    # Set jobs status
    updated = set_jobs_status(args.db, args.pattern, args.status, args.dry_run)
    
    # Exit with success if any jobs were found/updated
    sys.exit(0 if updated > 0 else 1)

if __name__ == "__main__":
    main()
