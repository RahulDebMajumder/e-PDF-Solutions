import pandas as pd
import os
from tqdm import tqdm
import json
from datetime import datetime
import requests
import pandas as pd
from reader.reader import Reader
import asyncio
from tabulate import tabulate


perfios_outputs = {}


def perfios_api(perfios_id):
    try:
        url = "https://api-marketplace.prod.growth-source.com:443/api/27650431-banking/operation/get-banking-statements-report/execute/banking/perfios/getReport/bs"

        payload = json.dumps({
            "perfiosTransactionId": perfios_id,
            "perfiosReportType": "JSON"
        })
        headers = {
            'accept': '*/*',
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        if response.status_code != 200:
            raise Exception()

        data = json.loads(response.content)

        return data['data']['jsonResponse']

    except Exception as e:
        print('Exception,', e)
        return None

def get_perfios_output(perfios_id):
    """
        Will return all the transactions and details associated with the file_name
    """
    # step 1: get perfios output
    perfios_output = perfios_outputs.get(perfios_id, None)
    if not perfios_output:
        perfios_output = perfios_api(perfios_id)
        perfios_outputs[perfios_id] = perfios_output

    # step 2: get all the account transactions
    account_no_to_transactions = {}
    if perfios_output:
        accountsXns = perfios_output['accountXns']
        for accountXns in accountsXns:
            account_number, account_transactions = None, []
            account_number = accountXns['accountNo']

            for transaction in accountXns['xns']:
                if isinstance(transaction['date'], str):
                    transaction['date'] = datetime.strptime(transaction['date'], "%Y-%m-%d")
                account_transactions.append(transaction)

            account_transactions_df = pd.DataFrame(account_transactions)
            account_transactions_df = account_transactions_df.rename(
                columns={'amount': 'Amount', 'date': 'Date', 'balance': 'Balance',
                         'category': 'Category', 'narration': 'Narration', 'chqNo': 'Ref No./Cheque No.'})

            account_no_to_transactions[account_number] = account_transactions_df

    return account_no_to_transactions


def get_parser_output(file_paths):
    reader_obj = Reader(source="e_PDF", ePDF_s3_paths=file_paths)
    output = asyncio.run(reader_obj.get_unified_data())
    return output

def get_eod_balances(transactions):
    """
    Extracts the End-of-Day (EoD) balances for each day from a single account's transactions.
    """
    if transactions.empty:
        return pd.DataFrame(columns=['Date', 'Balance'])  # Return an empty DataFrame if no transactions

    transactions['Date'] = pd.to_datetime(transactions['Date'])
    eod_balances = transactions.groupby('Date')['Balance'].last().reset_index()

    end_date = transactions['Date'].max()
    start_date = end_date - pd.DateOffset(months=12)  # Last 12 months
    date_range = pd.date_range(start=start_date, end=end_date)

    eod_df = pd.DataFrame({'Date': date_range})
    eod_df = eod_df.merge(eod_balances, on='Date', how='left').fillna(method='ffill')
    eod_df['Balance'] = eod_df['Balance'].fillna(method='bfill')

    return eod_df


def aggregate_eod_balances(transactions):
    """
    Aggregates the EoD balances across all accounts.
    """
    aggregated_balances = pd.DataFrame()

    for account, group in transactions.groupby('Account Number'):
        account_eod_balances = get_eod_balances(group)

        if aggregated_balances.empty:
            aggregated_balances = account_eod_balances
        else:
            # Sum the balances for matching dates across all accounts
            aggregated_balances = aggregated_balances.merge(account_eod_balances, on='Date', how='outer',
                                                            suffixes=('', '_tmp'))
            aggregated_balances.fillna(0, inplace=True)
            aggregated_balances['Balance'] = aggregated_balances['Balance'] + aggregated_balances.pop('Balance_tmp')

    # Ensure the 'Date' column is in datetime format and sort by 'Date'
    aggregated_balances['Date'] = pd.to_datetime(aggregated_balances['Date'])

    return aggregated_balances


def calculate_avg_eod_over_period(transactions, last_days=180):
    """
    Calculates the average EoD balance over the specified last days for each account individually,
    then sums these averages to get a final value.
    """
    final_avg_sum = 0  # Initialize the sum of averages

    for account, group in transactions.groupby('Account Number'):
        eod_balances = get_eod_balances(group)  # Use the existing function to get EoD balances

        # Filter for the last `last_days` days
        end_date = eod_balances['Date'].max()
        start_date = end_date - pd.DateOffset(days=last_days - 1)
        filtered_balances = eod_balances[
            (eod_balances['Date'] >= start_date) & (eod_balances['Date'] <= end_date)]

        # Calculate the average balance for the account over the specified period
        account_avg = filtered_balances['Balance'].mean()

        # Add the account's average to the final sum
        final_avg_sum += account_avg

    # The final_avg_sum now holds the sum of the average EoD balances of each account
    return final_avg_sum


def compare_output(perfios_transactions, parser_transactions, perfios_id):
    results_dir = 'results'
    # Ensures the 'results' directory exists in the current directory
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    # Constructs the full file path with the 'results' directory and the provided file name
    file_path = os.path.join(results_dir, f"{perfios_id}.xlsx")

    # Initial checks for empty DataFrames
    if perfios_transactions.empty:
        print('Perfios transactions are empty')
        return False
    if parser_transactions.empty:
        print('Parser transactions are empty')
        return False

    perfios_transactions = perfios_transactions[perfios_transactions['Amount'] != 0]
    parser_transactions = parser_transactions[parser_transactions['Amount'] != 0]

    # Finding the common start_date and end_date
    start_date = max(perfios_transactions['Date'].iloc[0], parser_transactions['Date'].iloc[0])
    end_date = min(perfios_transactions['Date'].iloc[-1], parser_transactions['Date'].iloc[-1])

    # Filtering both DataFrames between start_date and end_date
    perfios_transactions = perfios_transactions[
        (perfios_transactions['Date'] > start_date) & (perfios_transactions['Date'] < end_date)
        ]

    parser_transactions = parser_transactions[
        (parser_transactions['Date'] > start_date) & (parser_transactions['Date'] < end_date)
        ]

    # Calculate credit count, debit count, total credit, and total debit for perfios_transactions
    perfios_credit_count = round(perfios_transactions[perfios_transactions['Amount'] > 0]['Amount'].count(), 2)
    perfios_debit_count = round(perfios_transactions[perfios_transactions['Amount'] < 0]['Amount'].count(), 2)
    perfios_total_credit = round(perfios_transactions[perfios_transactions['Amount'] > 0]['Amount'].sum(), 2)
    perfios_total_debit = round(perfios_transactions[perfios_transactions['Amount'] < 0]['Amount'].sum(), 2)

    # Calculate credit count, debit count, total credit, and total debit for parser_transactions
    parser_credit_count = round(parser_transactions[parser_transactions['Amount'] > 0]['Amount'].count(), 2)
    parser_debit_count = round(parser_transactions[parser_transactions['Amount'] < 0]['Amount'].count(), 2)
    parser_total_credit = round(parser_transactions[parser_transactions['Amount'] > 0]['Amount'].sum(), 2)
    parser_total_debit = round(parser_transactions[parser_transactions['Amount'] < 0]['Amount'].sum(), 2)

    avg_credit_trans_l06m_perfio = (perfios_transactions[(perfios_transactions['Date'] >
                                                          (perfios_transactions['Date'].max() - pd.DateOffset(
                                                              days=180))) &
                                                         (perfios_transactions['Amount'] > 0)]['Amount'].mean()) * 30

    avg_credit_trans_l06m_parser = (parser_transactions[(parser_transactions['Date'] >
                                                         (parser_transactions['Date'].max() - pd.DateOffset(
                                                             days=180))) &
                                                        (parser_transactions['Amount'] > 0)]['Amount'].mean()) * 30

    avg_eod_l6m_perfios = calculate_avg_eod_over_period(perfios_transactions)
    avg_eod_l6m_parser = calculate_avg_eod_over_period(parser_transactions)

    all_match = (round(parser_total_credit) == round(perfios_total_credit) and
                 round(parser_total_debit) == round(perfios_total_debit) and
                 round(avg_credit_trans_l06m_perfio) == round(avg_credit_trans_l06m_parser) and
                 round(avg_eod_l6m_perfios) == round(avg_eod_l6m_parser))

    #print(all_match, parser_total_credit, perfios_total_credit, parser_total_debit, perfios_total_debit, avg_credit_trans_l06m_perfio, avg_credit_trans_l06m_parser, avg_eod_l6m_perfios)

    # Writing to Excel with multiple sheets
    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        perfios_transactions.to_excel(writer, sheet_name='Perfios Transactions', index=False)
        parser_transactions.to_excel(writer, sheet_name='Parser Transactions', index=False)

        # Creating summary DataFrame
        summary_data = {
            'Metric': ['All Match', 'Parser Total Credit', 'Perfios Total Credit', 'Parser Total Debit',
                       'Perfios Total Debit', 'Avg Credit Trans Last 6M Perfios', 'Avg Credit Trans Last 6M Parser',
                       'Avg EOD Last 6M Perfios', 'Avg EOD Last 6M Parser'],
            'Value': [all_match, parser_total_credit, perfios_total_credit, parser_total_debit, perfios_total_debit,
                      avg_credit_trans_l06m_perfio, avg_credit_trans_l06m_parser, avg_eod_l6m_perfios,
                      avg_eod_l6m_parser]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary Output', index=False)

    return [all_match, parser_total_credit, perfios_total_credit, parser_total_debit, perfios_total_debit,
            avg_credit_trans_l06m_perfio, avg_credit_trans_l06m_parser, avg_eod_l6m_perfios, avg_eod_l6m_parser]



def merge_accounts(account_no_to_df):
    dfs = []
    for account_no, value in account_no_to_df.items():
        # Assuming the DataFrame is the first item in the tuple
        if isinstance(value, pd.DataFrame):
            df = value
        else:
            df = value[1]
        df['Account Number'] = account_no
        dfs.append(df)
    merged_df = pd.concat(dfs)
    merged_df = merged_df.drop_duplicates()
    #merged_df = merged_df.sort_values(by='Date')
    return merged_df



def run(file_path):
    df = pd.read_csv(file_path)

    n = df.shape[0]
    output = []

    for i in tqdm(range(n)):
        try:
            directory_path = df.iloc[i]['file_path']
            directory_path = os.path.join(r'C:\Users\rahul.majumder\OneDrive - Protium Finance Limited\Desktop\Final testing',directory_path)
            perfios_id = df.iloc[i]['provider_ref_id']
            file_paths = os.listdir(directory_path)

            temp = []
            for file_path in file_paths:
                tempi = os.path.join(directory_path, file_path)
                temp.append(tempi)

            file_paths = temp

            perfios_output = get_perfios_output(perfios_id)
            parser_output = get_parser_output(file_paths)

            perfios_output = merge_accounts(perfios_output)
            parser_output = merge_accounts(parser_output)

            verdict = compare_output(perfios_output, parser_output,perfios_id)
            verdict.append(directory_path)
            output.append(verdict)
            if(i == 10):
                break
        except Exception as e:
            if (i == 10):
                break
            print(f"An exception occurred: {e}")
            pass

    output = pd.DataFrame(output, columns = ["match", "Parser total credit", "Perfios total credit", "Parser total debit", "Perfios total debit", "Avg credit Perfio", "Avg credit Parser", "Avg eod Perfios", "Avg eod Parser","Path"])

    return output


output=run(r'C:\Users\rahul.majumder\OneDrive - Protium Finance Limited\Desktop\Final testing\file_list.csv')


df = pd.DataFrame(output)

# Save the DataFrame to an Excel file
df.to_excel("example.xlsx", index=False)


