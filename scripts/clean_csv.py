import csv

def clean_csv(input_file, output_file):
    with open(input_file, mode='r', encoding='utf-8', newline='', escapechar='\\') as infile:
        reader = csv.reader(infile)
        with open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile)

            # Iterate over each row and filter out rows containing '"' or '$$'
            for row in reader:
                if not any('"' in cell or '$' in cell for cell in row):
                    writer.writerow(row)  # Write only rows that pass the filter