# scheduler
An autosys-like job scheduler


This repository provides an **AutoSys-like scheduler** implemented in Python. It consists of four main tools:

* **autosys.py**: A daemon that runs continuously, scheduling and executing jobs defined in the database.
* **jil.py**: A parser that reads JIL (Job Information Language) files and populates the SQLite database.
* **autorep.py**: A reporting utility that generates job status summaries and detailed reports.
* **setstat.py**: A command-line tool to bulk-update job statuses in the database.

The scheduler uses an SQLite database (`autosys_jobs.db`) to store job definitions and track their execution status.

## Directory Structure

```
├── errors/            # Standard error output files for jobs
├── logs/              # Daemon and parser log files
├── output/            # Standard output files for jobs
├── autosys.py         # Scheduler daemon
├── jil.py             # JIL parser and database loader
├── autorep.py         # Job reporting tool
├── setstat.py         # Bulk status updater
├── autosys_jobs.db    # SQLite database (auto-generated)
└── sample.jil         # Example JIL file syntax
```

> **Note:** Before running the scheduler, ensure the `errors`, `logs`, and `output` directories exist:
>
> ```bash
> mkdir -p errors logs output
> ```

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/Skyhawk1207/scheduler.git
   cd scheduler
   ```
2. **Move into repo directory**:
   
```bash
   cd scheduler
   ```

3. **Ensure you have Python 3.6+**. All dependencies use Python’s standard library.

## Usage

### 1. Daemon Scheduler (`autosys.py`)

Runs continuously and executes jobs based on their defined `start_times` and `condition` dependencies.

```bash
# Start the scheduler (daemon mode)
./autosys.py
```

* Looks for job definitions in `autosys_jobs.db`.
* Executes command and box jobs, enforcing dependencies.
* Logs activity to the console; redirect output if needed:

```bash
./autosys.py > logs/scheduler.log 2> logs/scheduler.err &
```

### 2. JIL Parser (`jil.py`)

Parses a JIL file (custom syntax) and inserts job definitions into the database.

```bash
# Parse a specific JIL file
./jil.py path/to/job_definitions.jil

# Or use the provided sample.jil
./jil.py sample.jil
```

### 3. Reporting Tool (`autorep.py`)

Provides job status reports similar to `autorep` in AutoSys.

```bash
# Summary report for all jobs
autorep.py -s

# Detailed report for a specific job (exact match)
autorep.py -J Job1

# Pattern match (wildcards % or _), with detail
autorep.py -J "Job%" -d
```

### 4. Bulk Status Updater (`setstat.py`)

Updates the `status` field for jobs matching an SQL LIKE pattern.

```bash
# Dry run to see which jobs would change
./setstat.py -p "Job%" -s SU -n

# Actually set status to "IN"
./setstat.py -p "Box1%" -s IN
```

## Sample JIL Syntax

```jil
/* Define a box job */
insert_job: Box1
job_type: b
start_times: "00:00"
description: "Container box job"

/* Define a command job */
insert_job: Job1
job_type: c
command: "ps -ef"
box_name: Box1
max_run_alarm: 5
description: "First job in box"
stdout: "output/job1_output.log"
stderr: "errors/job1_errors.log"

insert_job: Job2
job_type: c
command: "ls -l"
box_name: Box1
condition: s(Job1)
max_run_alarm: 5
stdout: "output/job2_output.log"
stderr: "errors/job2_errors.log"
```

## Logging & Error Handling

* **Scheduler logs**: Output on stdout/stderr (use `logs/` for persistence).
* **Job output**: Redirected to files under `output/` and `errors/`, based on JIL `stdout` and `stderr` settings.
* **Database errors**: Logged to console; verify `autosys_jobs.db` permissions.

## Contributing

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature-name`.
3. Commit your changes and push: `git push origin feature-name`.
4. Open a Pull Request.

Please adhere to PEP8 and include tests for new functionality.

## License

This project is licensed under the MIT License.
