"""
dbt Runner Script

Simple script to execute dbt commands for the orderly project.
"""

import subprocess
import sys
from pathlib import Path
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command: str, cwd: str = None) -> tuple[int, str, str]:
    """Run a command and return exit code, stdout, stderr."""
    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return 1, "", str(e)

def run_dbt_command(dbt_cmd: str, project_dir: Path) -> bool:
    """Run a dbt command in the dbt directory."""
    dbt_dir = project_dir / "dbt"
    
    if not dbt_dir.exists():
        logger.error(f"dbt directory not found: {dbt_dir}")
        return False
    
    logger.info(f"Running: dbt {dbt_cmd}")
    exit_code, stdout, stderr = run_command(f"dbt {dbt_cmd}", str(dbt_dir))
    
    if stdout:
        print(stdout)
    
    if stderr and exit_code != 0:
        logger.error(f"dbt command failed: {stderr}")
        return False
    
    logger.info(f"dbt {dbt_cmd} completed successfully")
    return True

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run dbt commands for orderly project")
    parser.add_argument(
        "command",
        choices=["debug", "deps", "run", "test", "docs", "clean", "full"],
        help="dbt command to run"
    )
    parser.add_argument(
        "--select",
        help="dbt model selection criteria"
    )
    parser.add_argument(
        "--project-dir",
        default=".",
        help="Project root directory (default: current directory)"
    )
    
    args = parser.parse_args()
    
    project_dir = Path(args.project_dir).resolve()
    logger.info(f"Project directory: {project_dir}")
    
    success = True
    
    if args.command == "full":
        # Run complete pipeline
        commands = ["deps", "debug", "run", "test", "docs generate"]
        for cmd in commands:
            if not run_dbt_command(cmd, project_dir):
                success = False
                break
        
        if success:
            logger.info("🎉 Full dbt pipeline completed successfully!")
            logger.info("📖 View documentation with: dbt docs serve")
    
    elif args.command == "docs":
        # Generate and serve docs
        if run_dbt_command("docs generate", project_dir):
            logger.info("📖 Starting documentation server...")
            logger.info("🌐 Open http://localhost:8080 in your browser")
            run_dbt_command("docs serve", project_dir)
    
    else:
        # Single command
        cmd = args.command
        if args.select:
            cmd += f" --select {args.select}"
        
        success = run_dbt_command(cmd, project_dir)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
