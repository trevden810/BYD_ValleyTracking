import pandas as pd
from utils.api import process_data

def test_bol_logic():
    # Test Case 1: order_C1 exists
    data1 = [{
        'order_C1': 'BOL123',
        'order_C2': 'BOL456',
        '_kp_job_id': 'JOB789',
        'job_date': '01/01/2023',
        'job_status': 'Open'
    }]
    df1 = process_data(data1)
    print(f"Case 1 (C1 exists): {df1['BOL_Number'].iloc[0]}")

    # Test Case 2: order_C1 empty, C2 exists
    data2 = [{
        'order_C1': '',
        'order_C2': 'BOL456',
        '_kp_job_id': 'JOB789',
        'job_date': '01/01/2023'
    }]
    df2 = process_data(data2)
    print(f"Case 2 (C1 empty): {df2['BOL_Number'].iloc[0]}")

    # Test Case 3: Both empty, Job ID exists
    data3 = [{
        'order_C1': '',
        'order_C2': '',
        '_kp_job_id': 'JOB789',
        'job_date': '01/01/2023'
    }]
    df3 = process_data(data3)
    print(f"Case 3 (Both empty): {df3['BOL_Number'].iloc[0]}")

if __name__ == "__main__":
    test_bol_logic()
