import pandas as pd
import re
def csv_to_excel(csv_files, excel_file):
    # create a Pandas Excel writer using xlsxwriter engine
    writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')

    # loop through each CSV file
    for csv_file in csv_files:
        # read the CSV file into a Pandas DataFrame
        df = pd.read_csv(csv_file)

        # get the filename without extension
        sheet_name = csv_file.split('/')[-1].split('.')[0]
        # shorten the sheet name if necessary
        if len(sheet_name) > 31:
            sheet_name = sheet_name[:31]
         # remove any invalid characters from the sheet name
        sheet_name = re.sub('[^A-Za-z0-9_]+', '', sheet_name)
        # write the DataFrame to a sheet in the Excel file
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    # save the Excel file
    writer.save()

import os
path = 'jewelery/datafolder'
print(os.listdir(path))
x = [path+'/'+y for y in os.listdir(path)]
csv_to_excel(x,path+'/excel.xlsx')
