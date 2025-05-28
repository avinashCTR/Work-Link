# #!/bin/bash

# set -e

# kinit -kt ~/keytabs/couture.keytab couture@COUTURE.AI

# # Default local folder path
# LOCAL_FOLDER_PATH="/data/sftp/jiomart/uploads"

# # Default HDFS directory
# HDFS_DIRECTORY="/data1/archive/avinash/GA_DATA"

# # Function to display usage information
# usage() {
#     echo "Usage: $0 [--save_path <HDFS_DIRECTORY>]"
#     exit 1
# }

# # Parse command-line arguments
# while [[ $# -gt 0 ]]; do
#     case "$1" in
#         --save_path)
#             HDFS_DIRECTORY="$2"
#             shift 2
#             ;;
#         *)
#             usage
#             ;;
#     esac
# done

# # Get today's date in YYYY-MM-DD format
# TODAY_DATE=$(date +"%Y-%m-%d")

# # Append today's date to the HDFS directory path
# HDFS_DIRECTORY="${HDFS_DIRECTORY}/${TODAY_DATE}"

# # Function to check if file was modified today
# is_file_modified_today() {
#     FILE_MOD_DATE=$(date -r "$1" +"%Y-%m-%d")
#     if [[ "$FILE_MOD_DATE" == "$TODAY_DATE" ]]; then
#         return 0  # File modified today
#     else
#         return 1  # File not modified today
#     fi
# }

# # Function to get the appropriate HDFS sub-directory based on the filename (case-insensitive matching)
# get_hdfs_subdirectory() {
#     local filename="$1"
    
#     # Use case-insensitive matching to check for substrings
#     if [[ "$filename" =~ [Z][S][R] ]]; then
#         echo "zsr"
#     elif [[ "$filename" =~ [P][O][S][I][T][I][O][N] ]]; then
#         echo "query_position"
#     elif [[ "$filename" =~ [P][I][D] ]]; then
#         echo "query_product_interactions"
#     elif [[ "$filename" =~ [F][R][E][Q][U][E][N][C][Y] ]]; then
#         echo "query_level"
#     else
#         echo "other"  # In case no match is found
#     fi
# }

# # Iterate over the files in the local folder
# for file in "$LOCAL_FOLDER_PATH"/*; do
#     if [ -f "$file" ]; then  # Check if it is a file
#         # Check if file was modified today
#         if is_file_modified_today "$file"; then
#             # Get the appropriate sub-directory for the file
#             SUB_DIR=$(get_hdfs_subdirectory "$(basename "$file")")
            
#             # Create the sub-directory in HDFS
#             HDFS_SUB_DIR="${HDFS_DIRECTORY}/${SUB_DIR}"
#             hdfs dfs -mkdir -p "$HDFS_SUB_DIR"
#             echo "Saving to HDFS sub-directory: $HDFS_SUB_DIR"

#             # Upload the file to the correct HDFS sub-directory
#             echo "Uploading $file to HDFS..."
#             hdfs dfs -put "$file" "$HDFS_SUB_DIR"
#             if [ $? -eq 0 ]; then
#                 echo "Successfully uploaded $file to $HDFS_SUB_DIR."
#             else
#                 echo "Error uploading $file to $HDFS_SUB_DIR."
#             fi
#         fi
#     fi
# done

#!/bin/bash

set -e

kinit -kt ~/keytabs/couture.keytab couture@COUTURE.AI

# Default local folder path
LOCAL_FOLDER_PATH="/data/sftp/jiomart/uploads"

# Default HDFS directory
HDFS_DIRECTORY="/data1/archive/avinash/GA_DATA"

# Function to display usage information
usage() {
    echo "Usage: $0 [--save_path <HDFS_DIRECTORY>]"
    exit 1
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --save_path)
            HDFS_DIRECTORY="$2"
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done

# Get today's date in YYYY-MM-DD format
TODAY_DATE=$(date +"%Y-%m-%d")

# Function to check if file was modified today
is_file_modified_today() {
    FILE_MOD_DATE=$(date -r "$1" +"%Y-%m-%d")
    if [[ "$FILE_MOD_DATE" == "$TODAY_DATE" ]]; then
        return 0  # File modified today
    else
        return 1  # File not modified today
    fi
}

# Function to get the appropriate HDFS sub-directory based on the filename (case-insensitive matching)
get_hdfs_subdirectory() {
    local filename="$1"
    
    # Use case-insensitive matching to check for substrings
    if [[ "$filename" =~ [Z][S][R] ]]; then
        echo "zsr"
    elif [[ "$filename" =~ [P][O][S][I][T][I][O][N] ]]; then
        echo "query_position"
    elif [[ "$filename" =~ [P][I][D] ]]; then
        echo "query_product_interactions"
    elif [[ "$filename" =~ [F][R][E][Q][U][E][N][C][Y] ]]; then
        echo "query_level"
    else
        echo "other"  # In case no match is found
    fi
}

# Iterate over the files in the local folder
for file in "$LOCAL_FOLDER_PATH"/*; do
    if [ -f "$file" ]; then  # Check if it is a file
        # Check if file was modified today
        if is_file_modified_today "$file"; then
            # Get the appropriate sub-directory for the file
            SUB_DIR=$(get_hdfs_subdirectory "$(basename "$file")")
            
            # Create the sub-directory in HDFS in the order: base/type/date
            HDFS_SUB_DIR="${HDFS_DIRECTORY}/${SUB_DIR}/${TODAY_DATE}"
            hdfs dfs -mkdir -p "$HDFS_SUB_DIR"
            echo "Saving to HDFS sub-directory: $HDFS_SUB_DIR"

            # Upload the file to the correct HDFS sub-directory
            echo "Uploading $file to HDFS..."
            hdfs dfs -put "$file" "$HDFS_SUB_DIR"
            if [ $? -eq 0 ]; then
                echo "Successfully uploaded $file to $HDFS_SUB_DIR."
            else
                echo "Error uploading $file to $HDFS_SUB_DIR."
            fi
        fi
    fi
done

