"""
Test runner script for PostgreSQL Narrow-to-Wide Table Migration
"""
import sys
import subprocess
import argparse
from pathlib import Path


def run_tests(test_type="all", verbose=False, coverage=True):
    """
    Run tests based on type
    
    Args:
        test_type: Type of tests to run (all, unit, integration)
        verbose: Enable verbose output
        coverage: Enable coverage reporting
    """
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add test path based on type
    if test_type == "unit":
        cmd.append("tests/unit/")
    elif test_type == "integration":
        cmd.append("tests/integration/")
    else:  # all
        cmd.append("tests/")
    
    # Add options
    if verbose:
        cmd.append("-v")
    
    if coverage:
        cmd.extend([
            "--cov=.",
            "--cov-report=html:htmlcov",
            "--cov-report=term-missing",
            "--cov-fail-under=80"
        ])
    
    # Add markers for specific test types
    if test_type == "unit":
        cmd.extend(["-m", "unit"])
    elif test_type == "integration":
        cmd.extend(["-m", "integration"])
    
    print(f"Running {test_type} tests...")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True)
        print(f"\n✅ {test_type.title()} tests passed!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {test_type.title()} tests failed with exit code {e.returncode}")
        return False


def run_specific_test(test_file):
    """
    Run a specific test file
    
    Args:
        test_file: Path to test file
    """
    cmd = ["python", "-m", "pytest", test_file, "-v"]
    
    print(f"Running specific test: {test_file}")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True)
        print(f"\n✅ Test {test_file} passed!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Test {test_file} failed with exit code {e.returncode}")
        return False


def run_linting():
    """Run code linting"""
    print("Running code linting...")
    
    # Check if flake8 is available
    try:
        cmd = ["python", "-m", "flake8", ".", "--max-line-length=100", "--ignore=E203,W503"]
        result = subprocess.run(cmd, check=True)
        print("✅ Code linting passed!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Code linting failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print("⚠️ flake8 not found, skipping linting")
        return True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Test runner for PostgreSQL Migration project")
    
    parser.add_argument("--type", choices=["all", "unit", "integration"], 
                       default="all", help="Type of tests to run")
    parser.add_argument("--file", help="Run specific test file")
    parser.add_argument("--no-coverage", action="store_true", 
                       help="Disable coverage reporting")
    parser.add_argument("--verbose", "-v", action="store_true", 
                       help="Enable verbose output")
    parser.add_argument("--lint", action="store_true", 
                       help="Run code linting")
    parser.add_argument("--all", action="store_true", 
                       help="Run all tests and linting")
    
    args = parser.parse_args()
    
    success = True
    
    if args.all:
        # Run linting
        if not run_linting():
            success = False
        
        # Run all tests
        if not run_tests("all", args.verbose, not args.no_coverage):
            success = False
    elif args.file:
        # Run specific test file
        if not run_specific_test(args.file):
            success = False
    elif args.lint:
        # Run only linting
        if not run_linting():
            success = False
    else:
        # Run tests
        if not run_tests(args.type, args.verbose, not args.no_coverage):
            success = False
    
    if success:
        print("\n🎉 All checks passed!")
        sys.exit(0)
    else:
        print("\n💥 Some checks failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
