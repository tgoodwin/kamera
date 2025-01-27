#! /usr/bin/env python3
import re

# keyword that the logline came from our instrumentation
SLEEVE_LOG_KEYWORD = "sleeveless"

LOG_TYPES = ["sleeve:controller-operation", "sleeve:object-version"]
pattern = re.compile(r'{"LogType": "(?:' + '|'.join(LOG_TYPES) + ')".*?"Observer": ".*?"}$')

def strip_logtype_from_lines(lines):
    return [strip_logtype(line) for line in lines]

def strip_logtype(line):
    return re.sub(pattern, "", line)

def process(infile, outfile):
    lines = infile.readlines()
    content = [line for line in lines if SLEEVE_LOG_KEYWORD in line]
    content = [line.split(SLEEVE_LOG_KEYWORD)[1].strip() for line in content]
    stripped = [strip_logtype(line) for line in content]

    for line in stripped:
        outfile.write(line + "\n")

def main():
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as infile:
            process(infile, sys.stdout)
    else:
        process(sys.stdin, sys.stdout)

if __name__ == "__main__":
    import sys
    main()

