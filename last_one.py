import os
import csv

def has_missing_value(row, columns_to_exclude=()):
    missing_values = ("NA", "NaN", "N/A", "None", "", " ", "-")
    for idx, cell in enumerate(row):
        if idx not in columns_to_exclude and cell.strip() in missing_values:
            return True
    return False

def combine_csv_files(input_directory, output_file):
    # Get a list of CSV files in the input directory
    csv_files = [f for f in os.listdir(input_directory) if f.endswith('.csv')]

    # Write the header to the output file
    header_written = False
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for idx, filename in enumerate(csv_files):
            print(f"{round((idx + 1) / len(csv_files), 4)}% done")
            input_file = os.path.join(input_directory, filename)
            with open(input_file, 'r', newline='') as infile:
                reader = csv.reader(infile)
                # Skip the header for each file except the first one
                if header_written:
                    next(reader)
                for row in reader:
                    # Specify the column indices to exclude from the missing value check
                    columns_to_exclude = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                    if not has_missing_value(row, columns_to_exclude):
                        writer.writerow(row)
                header_written = True

# Set up the file paths and the output file name
input_directory = 'outputs/'
output_file = 'combined_file_without_na2.csv'

# Call the function to combine the CSV files while skipping rows with missing values in other columns
combine_csv_files(input_directory, output_file)

