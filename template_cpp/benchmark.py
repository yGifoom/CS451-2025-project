#!/usr/bin/env python3

import argparse
import subprocess
import statistics
import sys
import os
import re
import shutil
from pathlib import Path

# Import correctness checker
try:
    from check_correctness import check_correctness
    CORRECTNESS_CHECKER_AVAILABLE = True
except ImportError:
    CORRECTNESS_CHECKER_AVAILABLE = False
    print("Warning: check_correctness.py not found, correctness checking disabled", file=sys.stderr)

def run_test(script_path, hosts_file, config_file, output_dir, use_valgrind=False):
    """Run a single test and extract execution time."""
    cmd = [script_path, hosts_file, config_file, output_dir]
    if use_valgrind:
        cmd.append("--valgrind")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        # Extract execution time from output
        # Look for "Execution time: X.XXXs (XXXms)" format
        match = re.search(r'Execution time: ([\d.]+)s \((\d+)ms\)', result.stdout)
        if match:
            return int(match.group(2))
        else:
            # Try alternative patterns
            match_alt = re.search(r'(\d+)ms', result.stdout)
            if match_alt:
                print(f"Warning: Using alternative pattern", file=sys.stderr)
                return int(match_alt.group(1))
            
            print(f"Warning: Could not extract execution time from output", file=sys.stderr)
            print(f"Looking for pattern: 'Execution time: X.XXXs (XXXms)'", file=sys.stderr)
            print(f"Last 800 chars of stdout:", file=sys.stderr)
            print(result.stdout[-800:], file=sys.stderr)
            print(f"\nLast 300 chars of stderr:", file=sys.stderr)
            print(result.stderr[-300:], file=sys.stderr)
            return None
    except subprocess.TimeoutExpired:
        print("Error: Test timed out after 5 minutes", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error running test: {e}", file=sys.stderr)
        return None

def main():
    parser = argparse.ArgumentParser(
        description='Run tests multiple times and compute execution time statistics'
    )
    parser.add_argument('hosts_file', help='Path to hosts file')
    parser.add_argument('config_file', help='Path to config file')
    parser.add_argument('-n', '--iterations', type=int, default=5,
                        help='Number of iterations to run (default: 5)')
    parser.add_argument('-o', '--output-dir', default=None,
                        help='Base output directory (default: auto-generated)')
    parser.add_argument('--valgrind', action='store_true',
                        help='Run with valgrind (will be much slower)')
    parser.add_argument('--script', default=None,
                        help='Path to test.sh script (default: auto-detect)')
    parser.add_argument('--check-correctness', action='store_true',
                        help='Check correctness of outputs after each run')
    parser.add_argument('--algorithm', choices=['pl', 'fifo', 'lattice'], default='pl',
                        help='Algorithm type for correctness checking (default: pl)')
    parser.add_argument('--keep-outputs', action='store_true',
                        help='Keep previous output directories (default: delete before starting)')
    
    args = parser.parse_args()
    
    # Find test.sh script
    if args.script:
        script_path = args.script
    else:
        script_dir = Path(__file__).parent
        script_path = script_dir / "test.sh"
    
    script_path = str(Path(script_path).resolve())
    
    if not os.path.isfile(script_path):
        print(f"Error: test.sh not found at {script_path}", file=sys.stderr)
        return 1
    
    # Convert paths to absolute
    hosts_file = str(Path(args.hosts_file).resolve())
    config_file = str(Path(args.config_file).resolve())
    
    # Verify input files
    if not os.path.isfile(hosts_file):
        print(f"Error: Hosts file not found: {hosts_file}", file=sys.stderr)
        return 1
    
    if not os.path.isfile(config_file):
        print(f"Error: Config file not found: {config_file}", file=sys.stderr)
        return 1
    
    # Determine base output directory
    if args.output_dir:
        base_output_dir = Path(args.output_dir)
    else:
        base_output_dir = Path(script_path).parent / "output"
    
    # Clean up previous outputs unless --keep-outputs is specified
    if not args.keep_outputs and base_output_dir.exists():
        print(f"Cleaning up previous outputs in {base_output_dir}...")
        try:
            shutil.rmtree(base_output_dir)
            print(f"Deleted {base_output_dir}")
        except Exception as e:
            print(f"Warning: Could not delete {base_output_dir}: {e}", file=sys.stderr)
    
    # Check consistency between config and hosts files if correctness checking is enabled
    if args.check_correctness and CORRECTNESS_CHECKER_AVAILABLE:
        print("Checking configuration consistency...")
        try:
            from check_correctness import read_config, read_hosts
            config = read_config(config_file)
            process_ids = read_hosts(hosts_file)
            
            if config.get('num_processes') is not None:
                if config['num_processes'] != len(process_ids):
                    print(f"ERROR: Configuration mismatch!", file=sys.stderr)
                    print(f"  Config file specifies {config['num_processes']} processes", file=sys.stderr)
                    print(f"  Hosts file contains {len(process_ids)} processes: {process_ids}", file=sys.stderr)
                    print(f"\nPlease fix the configuration before running benchmarks.", file=sys.stderr)
                    return 1
                print(f"✓ Configuration consistent: {len(process_ids)} processes")
            else:
                print("Warning: Could not validate num_processes from config", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Could not validate configuration: {e}", file=sys.stderr)
        print()
    
    print(f"Running {args.iterations} iterations...")
    print(f"Hosts file: {hosts_file}")
    print(f"Config file: {config_file}")
    if args.valgrind:
        print("Valgrind mode: ENABLED (execution will be much slower)")
    print()
    
    execution_times = []
    correctness_failures = 0
    
    for i in range(1, args.iterations + 1):
        # Create unique output directory for each iteration
        output_dir = str(base_output_dir / f"run_{i}")
        
        print(f"=== Iteration {i}/{args.iterations} ===")
        print(f"Output directory: {output_dir}")
        
        exec_time = run_test(script_path, hosts_file, config_file, output_dir, args.valgrind)
        
        if exec_time is not None:
            execution_times.append(exec_time)
            print(f"Execution time: {exec_time}ms ({exec_time/1000:.3f}s)")
            
            # Check correctness if enabled
            if args.check_correctness and CORRECTNESS_CHECKER_AVAILABLE:
                print("Checking correctness...", end=" ")
                success, errors = check_correctness(config_file, hosts_file, output_dir, args.algorithm)
                if success:
                    print("✓ PASSED")
                else:
                    print("✗ FAILED")
                    correctness_failures += 1
                    print("  Errors found:")
                    for component, error_list in errors.items():
                        print(f"    {component}:")
                        for error in error_list[:5]:  # Show first 5 errors
                            print(f"      - {error}")
                        if len(error_list) > 5:
                            print(f"      ... and {len(error_list) - 5} more errors")
                    
                    # Stop execution on correctness failure
                    print("\n" + "=" * 50)
                    print("EXECUTION STOPPED DUE TO CORRECTNESS FAILURE")
                    print("=" * 50)
                    print(f"Completed {i} iteration(s) before failure")
                    print(f"Successful runs before failure: {len(execution_times)}")
                    print(f"Correctness failures: {correctness_failures}")
                    return 2
            elif args.check_correctness and not CORRECTNESS_CHECKER_AVAILABLE:
                print("Correctness checking requested but check_correctness.py not available")
        else:
            print("Failed to get execution time")
        
        print()
    
    # Compute statistics
    if len(execution_times) == 0:
        print("Error: No successful test runs", file=sys.stderr)
        return 1
    
    print("=" * 50)
    print("RESULTS")
    print("=" * 50)
    print(f"Successful runs: {len(execution_times)}/{args.iterations}")
    if args.check_correctness and CORRECTNESS_CHECKER_AVAILABLE:
        print(f"Correctness failures: {correctness_failures}/{len(execution_times)}")
    print(f"Execution times (ms): {execution_times}")
    print()
    print(f"Mean:               {statistics.mean(execution_times):.2f}ms ({statistics.mean(execution_times)/1000:.3f}s)")
    print(f"Median:             {statistics.median(execution_times):.2f}ms ({statistics.median(execution_times)/1000:.3f}s)")
    
    if len(execution_times) > 1:
        print(f"Standard deviation: {statistics.stdev(execution_times):.2f}ms ({statistics.stdev(execution_times)/1000:.3f}s)")
        print(f"Min:                {min(execution_times)}ms ({min(execution_times)/1000:.3f}s)")
        print(f"Max:                {max(execution_times)}ms ({max(execution_times)/1000:.3f}s)")
    else:
        print("Standard deviation: N/A (need at least 2 runs)")
    
    # Return non-zero if correctness checks failed
    if args.check_correctness and correctness_failures > 0:
        print(f"\nWarning: {correctness_failures} run(s) failed correctness checks!", file=sys.stderr)
        return 2
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
