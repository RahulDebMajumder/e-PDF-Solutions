from reader.reader import Reader
from bank_parser.parser import Parser
import asyncio

reader_obj = Reader(
    source="ACCOUNT_AGGREGATOR",
    account_aggregator_id=550165,
)
reader_output = asyncio.run(reader_obj.get_unified_data())
parser_obj = Parser(reader_output)
transactions = parser_obj.get_AA_tables()
print(transactions)
