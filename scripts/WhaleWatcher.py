#!/usr/bin/env python3
import subprocess
import logging
from datetime import datetime
import sys
from pathlib import Path
import threading
import os

def setup_logging():
    """Setup logging configuration"""
    Path('logs').mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/daily_run_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def stream_output(pipe, logger, is_error=False):
    """Helper function to stream output from a pipe"""
    for line in pipe:
        line = line.strip()
        if line:
            if is_error:
                logger.error(line)
            else:
                logger.info(line)

def run_command(script_path, logger):
    """Run a command and log its output"""
    # Get the Python executable from the current environment
    python_executable = sys.executable
    logger.info(f"Using Python executable: {python_executable}")
    
    try:
        # Build the full command using list format
        command = [python_executable, script_path]
        logger.info(f"Running command: {' '.join(command)}")
        
        # Run command from project root
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Create threads to handle stdout and stderr simultaneously
        stdout_thread = threading.Thread(
            target=stream_output, 
            args=(process.stdout, logger)
        )
        stderr_thread = threading.Thread(
            target=stream_output, 
            args=(process.stderr, logger, True)
        )
        
        # Start threads
        stdout_thread.start()
        stderr_thread.start()
        
        # Wait for process to complete
        return_code = process.wait()
        
        # Wait for output threads to finish
        stdout_thread.join()
        stderr_thread.join()
        
        if return_code != 0:
            raise Exception(f"Command failed with return code {return_code}")
            
        logger.info(f"Command completed successfully")
        
    except Exception as e:
        logger.error(f"Error running command: {str(e)}")
        raise

def main():
    logger = setup_logging()
    logger.info("Starting daily whale monitoring pipeline")
    
    try:
        # Get the project root directory
        project_root = Path(__file__).parent.parent.absolute()
        logger.info(f"Project root: {project_root}")
        
        # Define scripts with proper path objects
        scripts = [
            project_root / "scripts" / "run_collection.py",
            project_root / "scripts" / "process_data.py",
            project_root / "scripts" / "run_alerts.py"
        ]
        
        # Run each script
        for script in scripts:
            run_command(str(script), logger)
        
        logger.info("Daily pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()