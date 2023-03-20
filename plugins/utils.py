import os
import sys
import csv
import json


class TusUtils:
    '''
    '''

    def __init__(self):
        pass

    def _split_json_file(
        self,
        infile,
        row_limit=100,
        output_path='.',
        output_name_template='output_%s.json'
    ):

        with open(infile, 'r', encoding='utf-8') as f:
            data = json.load(infile)
            for i in range(0, len(data), row_limit):
                current_out_path = os.path.join(
                output_path,
                output_name_template  % i
                )
                with open(current_out_path, 'w') as outfile:
                    json.dump(data[i:i+row_limit+1], outfile, ensure_ascii=False, indent=4)
                outfile.close()

    def _split_csv_file(
        self,
        infile_path,
        delimiter=',',
        row_limit=100,
        output_name_template='%s.csv',
        output_path='tmp/',
        keep_headers=True
    ):
        """
        Splits a CSV file into multiple pieces.
        source: https://gist.github.com/jrivero/1085501

        A quick bastardization of the Python CSV library.
        Arguments:
            `row_limit`: The number of rows you want in each output file. 10,000 by default.
            `output_name_template`: A %s-style template for the numbered output files.
            `output_path`: Where to stick the output files.
            `keep_headers`: Whether or not to print the headers in each output file.
        Example usage:

            >> from toolbox import csv_splitter;
            >> csv_splitter.split(open('/home/ben/input.csv', 'r'));

        """
        infile = open(infile_path, 'r', encoding='utf-8')

        reader = csv.reader(infile, delimiter=delimiter)
        current_piece = 1
        current_out_path = os.path.join(
            output_path,
            output_name_template  % current_piece
        )
        current_out_writer = csv.writer(open(current_out_path, 'w',  encoding='utf-8'), delimiter=delimiter)
        current_limit = row_limit
        if keep_headers:
            headers = next(reader)
            current_out_writer.writerow(headers)
        for i, row in enumerate(reader):
            if i + 1 > current_limit:
                current_piece += 1
                current_limit = row_limit * current_piece
                current_out_path = os.path.join(
                output_path,
                output_name_template  % current_piece
                )
                current_out_writer = csv.writer(open(current_out_path, 'w', encoding='utf-8'), delimiter=delimiter)
                if keep_headers:
                    current_out_writer.writerow(headers)
            current_out_writer.writerow(row)
        infile.close()

