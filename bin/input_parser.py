#!/usr/bin/env python3
import pandas as pd
import os
import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description="parsing input table and scanning file paths")
    parser.add_argument("--input", required=True, help="input table (xlsx/csv/tsv)")
    parser.add_argument("--col", default="Secondary Path", help="colname containing file paths")
    parser.add_argument("--scan_all", action="store_true", help="whether to scan all files in the directory")
    args = parser.parse_args()

    # 1. support several input formats
    try:
        if args.input.endswith('.xlsx'):
            df = pd.read_excel(args.input)
        elif args.input.endswith('.csv'):
            df = pd.read_csv(args.input)
        else:
            df = pd.read_csv(args.input, sep='\t')
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(1)

    # 2. scan files
    for folder_path in df[args.col]:
        if not os.path.exists(folder_path):
            continue
        
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # by default: calculating *.fq.gz only, unless use '--scan_all'
                if args.scan_all or file.endswith('.fq.gz'):
                    print(os.path.abspath(os.path.join(root, file)))

if __name__ == "__main__":
    main()
