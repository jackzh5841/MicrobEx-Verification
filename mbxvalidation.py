import ucx_core as ucx
import microbex as me
import pandas as pd
import hl7
# import mysql.connector
import os
# import sys
import re
from yachalk import chalk
import time 
import numpy as np
import math
from multiprocessing import Pool
import dataAquisition as da

def calculate_average(arr):
    if len(arr) == 0:
        return 0  # To handle an empty array, you can return 0 or handle it differently as needed.
    else:
        return sum(arr) / len(arr)
    
def process_for_ucx(parsed_record):  
    arr_ucx_hl7 = []

    str_parsed = str(parsed_record).lower()
    
    arr_hl7_segs = re.split(ucx.HL7_UCX_ID_REG, str_parsed)
    if len(arr_hl7_segs) != 3:
        print(chalk.red(f'In this report {len(arr_hl7_segs)} segs found'))
    
    reg_used = ucx.HL7_UCX_ID_REG

    if len(arr_hl7_segs) == 1: #likely this was caught by desc only
        arr_hl7_segs = re.split(ucx.HL7_UCX_DESC_REG, str_parsed)
        print(chalk.yellow(f'Resplit w/ DESC, in this report {len(arr_hl7_segs)} segs found: {arr_hl7_segs}'))
        reg_used = ucx.HL7_UCX_DESC_REG
    
    arr_length = []
    i = 0
    while i < len(arr_hl7_segs)-1 :
        if re.match(reg_used, arr_hl7_segs[i]):
            # now capture till where likely next culture heading is to avoid double culture in one ucx seg
            arr_sub_hl7_segs = re.split(ucx.HL7_CULTURE_REG, arr_hl7_segs[i+1])
            
            if len(arr_sub_hl7_segs) > 1:
                print(chalk.red('=====Woah double culture====='))
            
            lines = arr_hl7_segs[0].split('\n') # doesn't matter what [0] is, either '' or something useful 
            num_lines = len(lines)
            arr_length.append(num_lines)
            # print(f'splittedd into {num_lines} lines')
            ucx_line_string = "\n".join(lines[:min(num_lines, ucx.HL7_UCX_LENGTH)])    

            temp_str = arr_hl7_segs[i] + ucx_line_string 
            arr_ucx_hl7.append(temp_str)
            i = i+1 # remember the next one (useful part) is now processed 
        i = i + 1
    
    if 0 == len(arr_ucx_hl7) :
        print(chalk.red('No ucx reports found'))
    # else:
        # print(chalk.blue(f'In this report {len(arr_ucx_hl7)} ucx reports found'))
    # print(f'avg split length{calculate_average(arr_length)}')
    ucx.arr_hl7_seg_length.append(calculate_average(arr_length))
    return arr_ucx_hl7    

def process_hl7_reports_batch(res):
    df_shape = res.shape
    num_rows = df_shape[0]  # Number of rows
    # num_cols = df_shape[1]  # Number of columns
    # batch_res = res.pop()

    i = 0
    total_processed = 0   

    while total_processed < num_rows :
        tic = time.perf_counter()
        df_array = []
        parsed_array = []
    
        idx_range = min(num_rows, batch_size)
        for _ in range(0, idx_range):
            record = res.iloc[0]
            res = res.iloc[1:]

            arr_ucx_hl7 = []

            if ucx.SQL_MODE:
                #find all the individual ucx hl7 segs in this report
                arr_ucx_hl7 = process_for_ucx(hl7.parse(record[1]))
            else:
                arr_ucx_hl7 = [record[1]]
                
            for ucx_hl7 in arr_ucx_hl7:        
                d={'parsed_note': str(ucx_hl7), 'culture_id': record[0], 'visit_id': i} #culture_id is actually lab_id, visit_id is basically iteration index AYAYA
                i=i+1
                
                df=pd.DataFrame(data=d, index=[1])

                obj1= me.Microbex(df,
                            text_col='parsed_note',
                            culture_id_col='culture_id',
                            visit_id_col='visit_id'
                            )
                #old_stdout = sys.stdout
                #sys.stdout = open(os.devnull, 'w')
                ## see microbex.annotate() docstring for description of kwargs
                obj1.annotate(staph_neg_correction=False, 
                            specimen_col=None,
                            review_suggestions=False,
                            likelyneg_block_skip=False,
                            verbose=False
                            )

                #sys.stdout = old_stdout

                if ucx.OUTPUT_PARSED_TEXT:
                    parsed_array.append(obj1.annotated_data.loc[:,0])

                obj1.annotated_data.drop(obj1.annotated_data.columns[0], axis = 1, inplace = True)
                df_array.append(obj1.annotated_data)
                # print(obj1.annotated_data.head())

            # obj1.annotated_data.to_pickle("microbe.pkl")
        
        csv_file_name = ucx.DB_FOLDER + 'res_' + ucx.STR_TIME + '.csv'
        parsed_text_file_name = ucx.DB_FOLDER + 'parsed_' + ucx.STR_TIME + '.csv'
        pd.concat(df_array).to_csv(csv_file_name, mode='a', header=not os.path.exists(csv_file_name))
        
        if ucx.OUTPUT_PARSED_TEXT:
            pd.concat(parsed_array).to_csv(parsed_text_file_name, mode='a', header=not os.path.exists(parsed_text_file_name))

        #next loop
        # try:
        #     batch_res = curs_dev.fetchmany(batch_size)
        # except:
        #     print(chalk.red('disconnected???'))
        #     try:
        #         cnx_dev.reconnect(3, 5)
        #         batch_res = curs_dev.fetchmany(batch_size)
        #         print(chalk.yellow('reconnected to sql...'))
        #     except:
        #         print(chalk.red('failed to reconnect to sql...'))

        total_processed += idx_range
        toc = time.perf_counter()
        print(chalk.green(f'processed {idx_range} in {toc - tic:0.4f} seconds rate {(idx_range / (toc - tic)):0.2f} per sec, total processed {total_processed}, total time {toc - orig_tic:0.2f} remaininig {num_rows - total_processed}'))
        df = pd.DataFrame(ucx.arr_hl7_seg_length)
        if not df.empty:
            print(df.describe())
            print("DataFrame is not empty")
        else:
            print("DataFrame is empty")

    # obj = pd.read_pickle("microbe.pkl")
    # print (obj)
    print(f'done, last batch size {idx_range}')


# todo: rename res to df since it's pandas dataframe
res = da.getData()
batch_size = 100
total_size = res.shape[0]
# batch_res = curs_dev.fetchmany(batch_size)
print(chalk.blue(f'total # of cultures: {total_size}'))
num_batches = math.ceil(total_size/ batch_size)
num_batches_to_do = num_batches
arr_mp = np.array_split(res, num_batches)
del res

orig_tic = time.perf_counter() 

while (len(arr_mp)):
    tic = time.perf_counter()
    num_process = min(len(arr_mp), ucx.PROCESSOR_NUM)
    arr_mp_per_batch = arr_mp[0:num_process-1]
    arr_mp = arr_mp[num_process:]

    with Pool(num_process) as p:
        p.map(process_hl7_reports_batch, arr_mp_per_batch)
    
    toc =time.perf_counter()
    num_processed = num_process * batch_size
    num_batches_to_do -= 1
    print(chalk.bg_blue_bright(f'processed {num_processed} in {toc - tic:0.4f} seconds rate {(num_processed / (toc - tic)):0.2f} per sec, total time {toc - orig_tic:0.2f}, {num_batches_to_do} batches remaining'))