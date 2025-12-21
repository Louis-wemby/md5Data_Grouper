#!/usr/bin/env python3
import pandas as pd
import argparse
import math

def main():
    parser = argparse.ArgumentParser(description="collecting calculated results and grouping")
    parser.add_argument("--results", required=True, help="original csv files after merging")
    parser.add_argument("--limit_tb", type=float, default=11.0, help="TB limitation of each group")
    parser.add_argument("--prefix", default="result", help="prefix of output file")
    args = parser.parse_args()

    # 1. loading data (path, size, MD5)
    df = pd.read_csv(args.results, names=['Absolute Path', 'Size', 'MD5 Value'])
    
    # save the complete list (output A)
    full_list_name = f"{args.prefix}_full_list.xlsx"
    df.to_excel(full_list_name, index=False)

    # 2. grouping (output B)
    # 1 TB = 1024^4 Bytes
    limit_bytes = args.limit_tb * (1024**4)
    
    current_group = []
    current_size = 0
    group_count = 1

    for _, row in df.iterrows():
        file_size = row['Size']
        
        # if oversized, then save it and create a new group
        if current_size + file_size > limit_bytes and current_group:
            save_group(current_group, args.prefix, group_count)
            group_count += 1
            current_group = []
            current_size = 0
        
        current_group.append(row)
        current_size += file_size

    # save the last group
    if current_group:
        save_group(current_group, args.prefix, group_count)

def save_group(data_list, prefix, count):
    group_df = pd.DataFrame(data_list)
    output_name = f"{prefix}_group_{count:03d}.xlsx"
    group_df.to_excel(output_name, index=False)

if __name__ == "__main__":
    main()
