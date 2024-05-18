import pandas as pd

from reader.reader import Reader
import asyncio
from tabulate import tabulate

reader_obj = Reader(source="e_PDF", ePDF_s3_paths=[r"C:\Users\rahul.majumder\OneDrive - Protium Finance Limited\Desktop\bankinglib\ePDF Samples\9\3.pdf"])


output = asyncio.run(reader_obj.get_unified_data())
print('Hello World')
print(output)

#print(type(output['50200004835370'][0][1]))
# account_numbers = output.keys()
#
# for account_number in account_numbers:
#     result = output[account_number]
#     print(result)
print("END")
