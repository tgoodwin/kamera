#!/bin/bash

# Check if the input directory is provided as an argument
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <input_directory>"
  exit 1
fi

# Get the input directory from the first argument
input_dir="$1"

# Get the directory containing the shell script
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Path to your Python script
script_path="$script_dir/object_graph.py"

# Loop through all .trace files in the directory
for trace_file in "$input_dir"/*.trace; do
  # Check if there are any .trace files in the directory
  if [[ ! -e $trace_file ]]; then
    echo "No .trace files found in $input_dir"
    exit 1
  fi

  # Get the base name without the .trace extension
  base_name=$(basename "$trace_file" .trace)

  # Define the output file name with .png extension
  output_file="$input_dir/$base_name"

  # Execute the Python script with the infile and outfile arguments
  python3 "$script_path" "$trace_file" --outfile "$output_file" --mode synthesized

done