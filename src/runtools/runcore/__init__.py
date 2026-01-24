"""
Core component of the runtools package providing:
 1. Main constructs for job execution (run, phase, job, job instance)
 2. DB persistence with SQLite implementation for job history and state
 3. Environment connectors for local and remote job management

runtools is a framework for creating, executing, and managing job workflows.
Requires Python 3.12+.
"""

__version__ = "0.1.0"
