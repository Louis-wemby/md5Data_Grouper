version 1.0

# ==================================================================================
# Workflow: md5Data_Grouper (automated counting and grouping tool for sequencing data)
# Tool: Use fastMD5 (https://github.com/moold/fastMD5) for faster calculation
# Platform：BGI DCS Cloud (WDL)
# Author: Louis Xiong
# Date: 12/21/2025
# ===================================================================================

workflow md5Data_Grouper {
    input {
        File input_table             # input table (support format: xlsx/csv/tsv)
        String col_name = "Secondary Path"  # column containing paths to data in the input table 
        Float size_limit_tb = 11.0    # maximum capacity threshold for each group (unit: TB)
        String output_prefix = "result"
        Int cpu_per_task = 4         # fastMD5 thread numbers
        Boolean scan_all = false      # whether to calculate all formats of files (default: *.fq.gz only)
        Int speed_level = 1          # calculating speed level of fastmd5
        String fastmd5_bin = "/opt/conda/bin/myfastmd5"      # path to fastmd5 tool
        String docker_image = "/your/docker/image"      # docker image
    }

    # 1. parsing input table: extract absolute paths to data to be processed
    call ScanFiles {
        input:
            input_file = input_table,
            col = col_name,
            scan_all = scan_all,
            docker = docker_image
    }

    # 2. parallel calculating: counting the size of each file and calculating their md5/SHA-256 value
    scatter (file_path in ScanFiles.file_paths) {
        call CalcStats {
            input:
                file_path = file_path,
                threads = cpu_per_task,
                speed = speed_level,
                fastmd5_path = fastmd5_bin,
                docker = docker_image
        }
    }

    # 3. collecting and grouping
    call GroupResults {
        input:
            stats_files = CalcStats.stat_csv,
            size_limit = size_limit_tb,
            prefix = output_prefix,
            docker = docker_image
    }

    output {
        File full_list = GroupResults.full_list_xlsx
        Array[File] groups = GroupResults.group_xlsx
    }
}

# -----------------------------------------------------------------------------------
# Task 1: scanning & parsing
# -----------------------------------------------------------------------------------
task ScanFiles {
    input {
        File input_file
        String col
        Boolean scan_all
        String docker
    }
    command <<<
        python ~/bin/input_parser.py \
            --input "~{input_file}" \
            --col "~{col}" \
            ~{if scan_all then "--scan-all" else ""} > files.txt
    >>>
    output {
        Array[String] file_paths = read_lines("files.txt")
    }
    runtime {
        docker: docker
        memory: "2 GB"
        cpu: 1
    }
}

# -----------------------------------------------------------------------------------
# Task 2: calculating
# -----------------------------------------------------------------------------------
task CalcStats {
    input {
        String file_path
        Int threads
        Int speed
        String fastmd5_path
        String docker
    }
    command <<<
        export PATH="/opt/conda/bin:/usr/bin:/bin"
        ~{fastmd5_path} --help > tool_check.log 2>&1 || true
        sleep $((RANDOM % 60))
        # 1. obtain the accurate size of each file (Byte)
        SIZE=$(du -sbL ~{file_path} | awk '{print $1}')
        
        # 2. using fastMD5 to calculate
        MD5=$(~{fastmd5_path} -t ~{threads} -s ~{speed} -l ~{file_path} | awk '{print $1}')
        
        # 3. output counting results
        echo "~{file_path},$SIZE,$MD5" > stat.csv
    >>>
    output {
        File stat_csv = "stat.csv"
        File check_log = "tool_check.log"
    }
    runtime {
        docker: docker
        cpu: threads
        memory: "4 GB"
    }
}

# -----------------------------------------------------------------------------------
# Task 3: collecting all output and grouping
# -----------------------------------------------------------------------------------
task GroupResults {
    input {
        Array[File] stats_files
        Float size_limit
        String prefix
        String docker
    }
    command <<<
        # merging all output results
        cat ~{sep=' ' stats_files} > all_stats.raw.csv
        
        # grouping according to the size limit of each group
        python ~/bin/data_grouper.py \
            --results all_stats.raw.csv \
            --limit_tb ~{size_limit} \
            --prefix ~{prefix}
    >>>
    output {
        # output List A (complete) and List Bs (grouped)
        File full_list_xlsx = "~{prefix}_full_list.xlsx"
        Array[File] group_xlsx = glob("~{prefix}_group_*.xlsx")
    }
    runtime {
        docker: docker
        memory: "8 GB"
        cpu: 2
    }
}
