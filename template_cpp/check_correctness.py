#!/usr/bin/env python3
# filepath: /home/ygifoom/epfl/da/CS451-2025-project/template_cpp/check_correctness.py

import argparse
import sys
from pathlib import Path
from collections import defaultdict

def read_config(config_file):
    """Parse config file to extract parameters."""
    with open(config_file, 'r') as f:
        lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
    
    # Parse first line: num_messages [num_processes]
    # For FIFO: only num_messages
    # For Perfect Links: num_messages num_processes
    parts = lines[0].split()
    num_messages = int(parts[0])
    num_processes = int(parts[1]) if len(parts) > 1 else None
    
    return {
        'num_messages': num_messages,
        'num_processes': num_processes
    }

def read_hosts(hosts_file):
    """Parse hosts file to get list of process IDs."""
    process_ids = []
    with open(hosts_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                # Format: id ip port
                parts = line.split()
                process_ids.append(int(parts[0]))
    return sorted(process_ids)

def check_perfect_links_receiver(output_file, num_messages, process_ids, receiver_id):
    """Check receiver output for Perfect Links."""
    errors = []
    
    if not output_file.exists():
        return [f"Receiver output file not found: {output_file}"]
    
    # Track delivered messages: (sender_id, seq_num) -> count
    delivered = defaultdict(int)
    
    with open(output_file, 'r') as f:
        line_num = 0
        for line in f:
            line_num += 1
            line = line.strip()
            if not line:
                continue
            
            parts = line.split()
            if len(parts) != 3:
                errors.append(f"Line {line_num}: Invalid format '{line}' (expected 'd sender_id seq_num')")
                continue
            
            if parts[0] != 'd':
                errors.append(f"Line {line_num}: Expected 'd' but got '{parts[0]}'")
                continue
            
            try:
                sender_id = int(parts[1])
                seq_num = int(parts[2])
            except ValueError:
                errors.append(f"Line {line_num}: Invalid integers in '{line}'")
                continue
            
            # Check for duplicate delivery
            key = (sender_id, seq_num)
            delivered[key] += 1
            if delivered[key] > 1:
                errors.append(f"Message ({sender_id}, {seq_num}) delivered {delivered[key]} times (duplicate on line {line_num})")
    
    # Check all messages were delivered from all senders (all processes except receiver)
    for sender_id in process_ids:
        if sender_id == receiver_id:
            continue  # Receiver doesn't send to itself
        
        for seq_num in range(1, num_messages + 1):
            key = (sender_id, seq_num)
            if key not in delivered:
                errors.append(f"Message ({sender_id}, {seq_num}) was never delivered")
            elif delivered[key] == 0:
                errors.append(f"Message ({sender_id}, {seq_num}) has 0 deliveries")
    
    return errors

def check_perfect_links_sender(output_file, process_id, num_messages):
    """Check sender output for Perfect Links."""
    errors = []
    
    if not output_file.exists():
        return [f"Sender output file not found: {output_file}"]
    
    broadcasts = []
    
    with open(output_file, 'r') as f:
        line_num = 0
        for line in f:
            line_num += 1
            line = line.strip()
            if not line:
                continue
            
            parts = line.split()
            if len(parts) != 2:
                errors.append(f"Line {line_num}: Invalid format '{line}' (expected 'b seq_num')")
                continue
            
            if parts[0] != 'b':
                errors.append(f"Line {line_num}: Expected 'b' but got '{parts[0]}'")
                continue
            
            try:
                seq_num = int(parts[1])
            except ValueError:
                errors.append(f"Line {line_num}: Invalid integer in '{line}'")
                continue
            
            broadcasts.append(seq_num)
    
    # Check for correct sequence and no duplicates
    expected = list(range(1, num_messages + 1))
    
    if len(broadcasts) != num_messages:
        errors.append(f"Expected {num_messages} broadcasts, got {len(broadcasts)}")
    
    # Check for duplicates
    seen = set()
    for i, seq_num in enumerate(broadcasts, 1):
        if seq_num in seen:
            errors.append(f"Duplicate broadcast: message {seq_num} appears multiple times")
        seen.add(seq_num)
    
    # Check all messages were broadcast
    for seq_num in expected:
        if seq_num not in seen:
            errors.append(f"Message {seq_num} was never broadcast")
    
    # Check order
    if broadcasts != expected and len(broadcasts) == num_messages:
        errors.append(f"Broadcasts not in sequential order: {broadcasts[:10]}... (showing first 10)")
    
    return errors

def check_perfect_links(config, hosts_file, output_dir):
    """Check Perfect Links correctness."""
    num_messages = config['num_messages']
    num_processes_config = config['num_processes']
    
    process_ids = read_hosts(hosts_file)
    num_processes_actual = len(process_ids)
    
    # Validate that config specifies num_processes (required for Perfect Links)
    if num_processes_config is None:
        return {
            "Configuration Error": [
                f"Config file must specify number of processes for Perfect Links algorithm",
                f"Expected format: num_messages num_processes"
            ]
        }
    
    # Validate that config and hosts file match
    if num_processes_config != num_processes_actual:
        return {
            "Configuration Mismatch": [
                f"Config file specifies {num_processes_config} processes, "
                f"but hosts file contains {num_processes_actual} processes",
                f"Processes in hosts file: {process_ids}"
            ]
        }
    
    # Receiver is the process with highest ID
    receiver_id = max(process_ids)
    
    all_errors = {}
    
    # Check receiver - using proc_{id}.output format
    receiver_output = Path(output_dir) / f"proc_{receiver_id}.output"
    receiver_errors = check_perfect_links_receiver(
        receiver_output, num_messages, process_ids, receiver_id
    )
    if receiver_errors:
        all_errors[f"Process {receiver_id} (receiver)"] = receiver_errors
    
    # Check senders - using proc_{id}.output format
    for process_id in process_ids:
        if process_id == receiver_id:
            continue
        
        sender_output = Path(output_dir) / f"proc_{process_id}.output"
        sender_errors = check_perfect_links_sender(sender_output, process_id, num_messages)
        if sender_errors:
            all_errors[f"Process {process_id} (sender)"] = sender_errors
    
    return all_errors

def check_fifo_broadcast(config, hosts_file, output_dir):
    """Check FIFO Broadcast correctness."""
    num_messages = config['num_messages']
    process_ids = read_hosts(hosts_file)
    
    all_errors = {}
    
    # Parse all output files first
    broadcasts = {}  # process_id -> list of broadcast msg_ids
    deliveries = {}  # process_id -> {sender_id -> list of delivered msg_ids}
    
    for process_id in process_ids:
        output_file = Path(output_dir) / f"proc_{process_id}.output"
        
        # Initialize even if file doesn't exist
        broadcasts[process_id] = []
        deliveries[process_id] = defaultdict(list)
        
        if not output_file.exists():
            all_errors[f"Process {process_id}"] = [f"Output file not found: {output_file}"]
            continue
        
        with open(output_file, 'r') as f:
            line_num = 0
            for line in f:
                line_num += 1
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split()
                
                if parts[0] == 'b':
                    # Broadcast entry: b msg_id
                    if len(parts) != 2:
                        if f"Process {process_id}" not in all_errors:
                            all_errors[f"Process {process_id}"] = []
                        all_errors[f"Process {process_id}"].append(
                            f"Line {line_num}: Invalid broadcast format '{line}' (expected 'b msg_id')"
                        )
                        continue
                    try:
                        msg_id = int(parts[1])
                        broadcasts[process_id].append(msg_id)
                    except ValueError:
                        if f"Process {process_id}" not in all_errors:
                            all_errors[f"Process {process_id}"] = []
                        all_errors[f"Process {process_id}"].append(
                            f"Line {line_num}: Invalid integer in '{line}'"
                        )
                
                elif parts[0] == 'd':
                    # Delivery entry: d sender_id msg_id
                    if len(parts) != 3:
                        if f"Process {process_id}" not in all_errors:
                            all_errors[f"Process {process_id}"] = []
                        all_errors[f"Process {process_id}"].append(
                            f"Line {line_num}: Invalid delivery format '{line}' (expected 'd sender_id msg_id')"
                        )
                        continue
                    try:
                        sender_id = int(parts[1])
                        msg_id = int(parts[2])
                        deliveries[process_id][sender_id].append(msg_id)
                    except ValueError:
                        if f"Process {process_id}" not in all_errors:
                            all_errors[f"Process {process_id}"] = []
                        all_errors[f"Process {process_id}"].append(
                            f"Line {line_num}: Invalid integers in '{line}'"
                        )
                else:
                    if f"Process {process_id}" not in all_errors:
                        all_errors[f"Process {process_id}"] = []
                    all_errors[f"Process {process_id}"].append(
                        f"Line {line_num}: Unknown entry type '{parts[0]}' (expected 'b' or 'd')"
                    )
    
    # Check each process
    for process_id in process_ids:
        if f"Process {process_id}" in all_errors and "Output file not found" in all_errors[f"Process {process_id}"][0]:
            continue  # Skip if file doesn't exist
        
        if f"Process {process_id}" in all_errors:
            continue  # Skip if already has parsing errors
        
        errors = []
        
        # Check broadcasts are in order and complete
        expected_broadcasts = list(range(1, num_messages + 1))
        if broadcasts[process_id] != expected_broadcasts:
            if len(broadcasts[process_id]) != num_messages:
                errors.append(f"Expected {num_messages} broadcasts, got {len(broadcasts[process_id])}")
            
            # Check for missing broadcasts
            broadcast_set = set(broadcasts[process_id])
            for msg_id in expected_broadcasts:
                if msg_id not in broadcast_set:
                    errors.append(f"Message {msg_id} was never broadcast")
            
            # Check for duplicates
            seen = set()
            for msg_id in broadcasts[process_id]:
                if msg_id in seen:
                    errors.append(f"Duplicate broadcast: message {msg_id}")
                    break  # Only report once
                seen.add(msg_id)
            
            # Check for order
            if broadcasts[process_id] != sorted(broadcasts[process_id]):
                errors.append(f"Broadcasts not in ascending order")
        
        # Check FIFO ordering for deliveries from each sender
        for sender_id, delivered_msgs in deliveries[process_id].items():
            # Check messages are in ascending order (FIFO property)
            if delivered_msgs != sorted(delivered_msgs):
                errors.append(f"FIFO violation: messages from process {sender_id} not delivered in order")
            
            # Check for duplicates in deliveries
            if len(delivered_msgs) != len(set(delivered_msgs)):
                duplicates = [msg for msg in set(delivered_msgs) if delivered_msgs.count(msg) > 1]
                errors.append(f"Duplicate deliveries from process {sender_id}: {duplicates[:5]}")
        
        # Check that delivered messages were broadcast
        for sender_id, delivered_msgs in deliveries[process_id].items():
            if sender_id not in broadcasts:
                errors.append(f"Process {sender_id} delivered messages but has no broadcast record")
                continue
            
            broadcast_set = set(broadcasts[sender_id])
            for msg_id in delivered_msgs:
                if msg_id not in broadcast_set:
                    errors.append(f"Delivered message ({sender_id}, {msg_id}) was never broadcast by process {sender_id}")
                    break  # Only report first occurrence
        
        if errors:
            all_errors[f"Process {process_id}"] = errors
    
    # Check agreement: if one process delivered a message, all must deliver it
    # Collect all (sender_id, msg_id) pairs that were delivered by at least one process
    all_delivered = set()
    for process_id in process_ids:
        if process_id not in deliveries:
            continue
        for sender_id, msgs in deliveries[process_id].items():
            for msg_id in msgs:
                all_delivered.add((sender_id, msg_id))
    
    # Check each process has delivered all messages
    for process_id in process_ids:
        if f"Process {process_id}" in all_errors and "Output file not found" in str(all_errors[f"Process {process_id}"]):
            continue  # Skip missing processes
        
        errors = all_errors.get(f"Process {process_id}", [])
        
        for sender_id, msg_id in all_delivered:
            if msg_id not in deliveries[process_id].get(sender_id, []):
                errors.append(f"Message ({sender_id}, {msg_id}) was delivered by some process but not by this one")
        
        if errors:
            all_errors[f"Process {process_id}"] = errors
    
    # Check that all broadcasts are delivered by all other processes
    for sender_id in process_ids:
        if sender_id not in broadcasts:
            continue
        
        for msg_id in broadcasts[sender_id]:
            for receiver_id in process_ids:
                if receiver_id == sender_id:
                    continue  # Don't check self-delivery
                
                if receiver_id not in deliveries:
                    continue  # Skip if receiver has no deliveries recorded
                
                if msg_id not in deliveries[receiver_id].get(sender_id, []):
                    if f"Process {receiver_id}" not in all_errors:
                        all_errors[f"Process {receiver_id}"] = []
                    all_errors[f"Process {receiver_id}"].append(
                        f"Message ({sender_id}, {msg_id}) was broadcast but never delivered"
                    )
    
    return all_errors

def check_lattice_agreement(config, hosts_file, output_dir):
    """Check Lattice Agreement correctness (not implemented)."""
    return {"Lattice Agreement": ["Not implemented yet"]}

def check_correctness(config_file, hosts_file, output_dir, algorithm='pl'):
    """
    Check correctness of outputs. Can be called from other scripts.
    Returns (success: bool, errors: dict)
    """
    try:
        config = read_config(config_file)
    except Exception as e:
        return False, {"Config Error": [str(e)]}
    
    if algorithm == 'pl':
        errors = check_perfect_links(config, hosts_file, output_dir)
    elif algorithm == 'fifo':
        errors = check_fifo_broadcast(config, hosts_file, output_dir)
    elif algorithm == 'lattice':
        errors = check_lattice_agreement(config, hosts_file, output_dir)
    else:
        return False, {"Error": [f"Unknown algorithm: {algorithm}"]}
    
    return len(errors) == 0, errors

def main():
    parser = argparse.ArgumentParser(
        description='Check correctness of distributed algorithm outputs'
    )
    parser.add_argument('config_file', help='Path to config file')
    parser.add_argument('hosts_file', help='Path to hosts file')
    parser.add_argument('output_dir', help='Directory containing output files')
    parser.add_argument('algorithm', choices=['pl', 'fifo', 'lattice'],
                        help='Algorithm to check: pl (Perfect Links), fifo (FIFO Broadcast), lattice (Lattice Agreement)')
    
    args = parser.parse_args()
    
    success, errors = check_correctness(
        args.config_file, 
        args.hosts_file, 
        args.output_dir, 
        args.algorithm
    )
    
    # Report results
    if success:
        print("✓ All correctness checks passed!")
        return 0
    else:
        print("✗ Correctness check FAILED")
        print("=" * 60)
        for component, error_list in errors.items():
            print(f"\n{component}:")
            for error in error_list[:20]:  # Limit to first 20 errors per component
                print(f"  - {error}")
            if len(error_list) > 20:
                print(f"  ... and {len(error_list) - 20} more errors")
        return 1

if __name__ == "__main__":
    sys.exit(main())