# compare the output from MicrobEx to cns_with_lab_id.csv
# csv, rachael, 2018 all refer to the batch of results from Rachael 

import pandas as pd
import ucx_core as ucx
import dataAquisition as da
from yachalk import chalk
import re
from sklearn.metrics import confusion_matrix
import os
import glob
import os
import shutil
import time


def process_file_path(file_path):
    if os.path.exists(file_path):
    # Rename the current file with its creation time and date
        timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime(os.path.getctime(file_path)))
        new_file_path = file_path + "_" + timestamp
        shutil.move(file_path, new_file_path)

def get_csv_file_path():
    folder_path = ucx.DB_FOLDER
    file_pattern = 'res_*.csv'
    file_paths = glob.glob(os.path.join(folder_path, file_pattern))
    latest_file_path = max(file_paths, key=os.path.getmtime)
    return latest_file_path

# Function to extract regex match result
def extract_regex_match(comment):
    match = re.search(ucx.re_abx_pattern, comment, flags=re.IGNORECASE)
    if match:
        return match.group()
    else:
        return ''


print(chalk.green('checking against 2018 human categorization results...'))
neg_0 = ['no growth', 'mixed flora/contaminated']

# Read the CSV file
# df_cns_w_id = pd.read_csv(ucx.CSV_CNS_LABID_FILE_PATH, usecols=['Lab_ID', 'organism_isolated'])
df_cns_w_id = pd.read_sas(ucx.SAS_CNS_LABID_FILE_PATH)[['Lab_ID', 'organism_isolated']]
#print(df_cns_w_id.head())

#TODO: just assuming the latest file is the correct one
res_csv_path = get_csv_file_path()
print(f'res_csv_path: {res_csv_path}')
df_res = pd.read_csv(res_csv_path, usecols=['culture_id', 'pos_culture_status'])

# Merge the two dataframes
temp_df = pd.merge(df_cns_w_id, df_res, left_on='Lab_ID', right_on='culture_id', how='outer', indicator=True)

# find lab_ids that are in df_cns_w_id but not in df_res
unique_to_df_cns_w_id = temp_df[temp_df['_merge'] == 'left_only'].copy()['Lab_ID']

# get the sas raw data from sql
df_ucx = da.getData(0)
# print(df_ucx.head())
                                                       
missing_lab_ids = df_cns_w_id[~df_cns_w_id['Lab_ID'].isin(df_ucx['lab_id'])]['Lab_ID']
if len(missing_lab_ids) > 0:
    print(chalk.yellow(f"len(missing_lab_ids): {len(missing_lab_ids)}"))
    print(missing_lab_ids)
else:
    print('All lab_ids in df_cns_w_id are in df_ucx.')

if len(unique_to_df_cns_w_id) > 0:
    print(chalk.yellow(f'len(unique_to_df_cns_w_id): {len(unique_to_df_cns_w_id)}'))
    # print(unique_to_df_cns_w_id.head())
    unique_to_df_cns_reports = pd.merge(unique_to_df_cns_w_id, df_ucx, left_on='Lab_ID', right_on='lab_id', how='inner')
    if len(unique_to_df_cns_reports) > 0:
        print(chalk.yellow(f'len(unique_to_df_cns_reports): {len(unique_to_df_cns_reports)}'))

        process_file_path(ucx.CSV_UNIQUE_TO_DF_CNS)
        unique_to_df_cns_reports.to_csv(ucx.CSV_UNIQUE_TO_DF_CNS, index=False)
        print(chalk.yellow(f'unique_to_df_cns_reports written to {ucx.CSV_UNIQUE_TO_DF_CNS}'))
    else:
        print(chalk.red(f'No unique_to_df_cns_reports found even though unique_to_df_cns_w_id is {unique_to_df_cns_w_id}.'))
else:
    print('No unique_to_df_cns_w_id found.')


# Rows that are in both dataframes
merged_df = temp_df[temp_df['_merge'] == 'both'].copy()
# print(merged_df.head())
print(f'len(merged_df): {len(merged_df)}')

del temp_df
del df_ucx
del df_cns_w_id
del df_res

# Add a new column 'human_pos_culture_status'
merged_df['human_pos_culture_status'] = merged_df['organism_isolated'].apply(lambda x: 0 if any(neg in x.decode() for neg in neg_0) else 1)
#print(merged_df.head())

merged_df['res_correct_to_2018'] = merged_df['human_pos_culture_status'] == merged_df['pos_culture_status']
# print(merged_df.head())

# Print the number of correct and incorrect results
num_correct = merged_df['res_correct_to_2018'].sum()
print(f"Correct: {num_correct}")

# Filter incorrect rows
incorrect_df = merged_df[(merged_df['culture_id'].notnull()) & (merged_df['res_correct_to_2018'] == False)]
# print(incorrect_df.head())
num_incorrect = incorrect_df.count()[0]

print(f"Incorrect: {num_incorrect}")  
print(f"correct ratio {num_correct / (num_correct + num_incorrect):.2f}")  


print(chalk.green('\nChecking against inbox comment results...'))
neg_1 = ['no growth', 'contam', 'mixed', 'neg', 'no uti', 'no bacteria', 'no bacteria seen', 'no bacteria growth', 'no bacteria isolated', 'no bacteria present']
pos_1 = ['pos', 'sensitive', 'resistant', 'resist', 'has uti', 'on abx', 'on antibiotics', 'on antibiotic']

df_inbox = da.getData(1)
df_inbox.sort_values(by=['lab_id'], inplace=True)
orig_df_inbox = df_inbox.copy()

df_inbox = df_inbox[df_inbox['lab_id'].isin(merged_df['Lab_ID'])]

num_res = merged_df.count()[0]

if ucx.const_inbox_all_comments_mode:
    print(chalk.yellow('const_inbox_all_comments_mode is True.'))
    # Group df_inbox by 'lab_id' and join the comments together
    df_inbox_grouped = df_inbox.groupby('lab_id')['Comment'].apply(lambda x: ' '.join(str(i).lower() if i and str(i).lower() not in ['none', 'null'] else '' for i in x)).reset_index()
    
    # Create a dictionary mapping 'lab_id' to 'Comment' in df_inbox_grouped
    inbox_dict = df_inbox_grouped.set_index('lab_id')['Comment'].to_dict()

    #print(f'len(inbox_dict): {len(inbox_dict)}')

    # Vectorized operations to create 'comment' column in merged_df
    merged_df.loc[:, 'comment'] = merged_df['Lab_ID'].map(inbox_dict)

    # Vectorized operations to create 'inbox_res' column in merged_df
    # use a two bit method 
    merged_df['inbox_res'] = 0b00 # lowest bit neg, higher bit: pos
    merged_df.loc[merged_df['comment'].str.contains('|'.join(neg_1), na=False, case=False), 'inbox_res'] |= 0b01
    merged_df.loc[merged_df['comment'].str.contains('|'.join(pos_1), na=False, case=False), 'inbox_res'] |= 0b10
    # Vectorized operation to match the regex pattern in ucx
    merged_df.loc[merged_df['comment'].str.contains(ucx.re_abx_pattern, na=False, case=False, regex=True), 'inbox_res'] |= 0b10
    # Apply the function to create 'regex_match' column in merged_df
    merged_df['regex_match'] = merged_df['comment'].apply(extract_regex_match)
else:
    print(chalk.yellow('Latest best comment mode. const_inbox_all_comments_mode is False.'))
    # Group df_inbox by 'lab_id' and join the comments together
    df_inbox = df_inbox[df_inbox['Comment'].notnull()]
    df_inbox['inbox_res'] = 0b00 # lowest bit neg, higher bit: pos
    df_inbox.loc[df_inbox['Comment'].str.contains('|'.join(neg_1), na=False, case=False), 'inbox_res'] |= 0b01
    df_inbox.loc[df_inbox['Comment'].str.contains('|'.join(pos_1), na=False, case=False), 'inbox_res'] |= 0b10
    # Vectorized operation to match the regex pattern in ucx
    df_inbox.loc[df_inbox['Comment'].str.contains(ucx.re_abx_pattern, na=False, case=False, regex=True), 'inbox_res'] |= 0b10
    df_inbox['regex_match'] = df_inbox['Comment'].apply(extract_regex_match)
    #print(df_inbox.head())

    inbox_dict = df_inbox.groupby('lab_id').apply(lambda x: x.to_dict('records')).to_dict()
    #print(inbox_dict)

    inbox_dict_filtered = {}
    for lab_id, data in inbox_dict.items():
        for item in data:
            if item['inbox_res'] & 0b01 or item['inbox_res'] & 0b10:
                if lab_id not in inbox_dict_filtered or item['timestamp'] > inbox_dict_filtered[lab_id]['timestamp']:
                    inbox_dict_filtered[lab_id] = item
            elif lab_id not in inbox_dict_filtered:
                inbox_dict_filtered[lab_id] = item  # just say a random entry
    
    if len(inbox_dict_filtered) != merged_df.shape[0]:
        print(chalk.red(f'len(inbox_dict_filtered) {len(inbox_dict_filtered)} != merged_df.shape[0] {merged_df.shape[0]} Lab_ID.unique(): {merged_df["Lab_ID"].nunique()}')) 
    
    update_df = pd.DataFrame.from_dict(inbox_dict_filtered, orient='index')

    # Merge update_df with merged_df based on 'lab_id'
    merged_df = pd.merge(merged_df, update_df[['inbox_res', 'Comment', 'regex_match']], left_on='Lab_ID', right_index=True, how='left')
    # Change column name 'Comment' to 'comment' in merged_df
    merged_df.rename(columns={'Comment': 'comment'}, inplace=True)

    

#print(merged_df.head()  )
#print(merged_df.columns)
merged_df['inbox_res'] = merged_df['inbox_res'].map({0b00: 2, 0b01: 0, 0b10: 1, 0b11: 3, None:4}) # 00: neither pos nor neg (2), 01: neg (0), 10: pos (1), 11: both pos and neg (3), no comment (4)

# Vectorized operations to create other columns in merged_df
merged_df['inbox_correct_to_2018'] = merged_df.apply(lambda row: row['inbox_res'] if row['inbox_res'] >= 2 else row['inbox_res'] == row['human_pos_culture_status'], axis=1)
merged_df['res_correct_to_inbox'] = merged_df['inbox_res'] == merged_df['pos_culture_status']
merged_df['res_correct_to_both'] = (merged_df['pos_culture_status'] == merged_df['human_pos_culture_status']) & (merged_df['pos_culture_status'] == merged_df['inbox_res'])
merged_df['res_correct_to_either'] = (merged_df['pos_culture_status'] == merged_df['human_pos_culture_status']) | (merged_df['pos_culture_status'] == merged_df['inbox_res'])

print(f'res_correct_to_both: {merged_df["res_correct_to_both"].sum()}')
print(f'res_correct_to_either: {merged_df["res_correct_to_either"].sum()}')

# Drop the 'culture_id' column
merged_df.drop('culture_id', axis=1, inplace=True)

regex_column = merged_df.pop('regex_match')
merged_df['regex_match'] = regex_column

# Move the 'comment' column to the last position
comment_column = merged_df.pop('comment')
merged_df['comment'] = comment_column


process_file_path(ucx.HUMAN_VALIDATED_RES_FILE_PATH)

merged_df.to_csv(ucx.HUMAN_VALIDATED_RES_FILE_PATH, index=False)

actual_labels = merged_df['human_pos_culture_status']
predicted_labels = merged_df['pos_culture_status']

# Calculate the confusion matrix
print(chalk.green("\nConfusion Matrix based on 2018 labeled result:"))
conf_matrix = confusion_matrix(actual_labels, predicted_labels)
conf_matrix_df = pd.DataFrame(conf_matrix, columns=['Predicted Negative', 'Predicted Positive'], index=['Actual Negative', 'Actual Positive'])
print(conf_matrix_df)
print('Note: "actual" = 2018 labeled result, "predicted" = result from Microbex')

# Calculating Sensitivity and Specificity
TP = conf_matrix[1, 1]  # True Positives
TN = conf_matrix[0, 0]  # True Negatives
FP = conf_matrix[0, 1]  # False Positives
FN = conf_matrix[1, 0]  # False Negatives

sensitivity = TP / (TP + FN)
specificity = TN / (TN + FP)
ppv = TP / (TP + FP)
npv = TN / (TN + FN)
f1_score = 2 * (ppv * sensitivity) / (ppv + sensitivity)

print(f"Sensitivity: {sensitivity:.3f}")
print(f"Specificity: {specificity:.3f}")
print(f"PPV (Positive Predictive Value): {ppv:.3f}")
print(f"NPV (Negative Predictive Value): {npv:.3f}")
print(f"F1 Score: {f1_score:.3f}")


# inbox statistics ===========================================================
print(chalk.green("\nInbox statistics:"))
#print(orig_df_inbox.head())

print(f'len(orig_df_inbox): {len(orig_df_inbox)}')

# Get all unique lab_ids in merged_df
merged_df_lab_ids = merged_df['Lab_ID'].unique()

# Get a boolean Series representing whether each lab_id in orig_df_inbox is in merged_df_lab_ids
common_lab_ids = orig_df_inbox['lab_id'].isin(merged_df_lab_ids)

# Use this boolean Series to index orig_df_inbox and get the size of the dictionary
num_total_comment_entries = common_lab_ids.sum()

# Count the size of the resulting dictionary
print(f"Total corresponding inbox comments: {num_total_comment_entries} avg comments per lab_id: {num_total_comment_entries / merged_df.shape[0]:.2f}")
# new stats portion
# Calculate the size of each entry
sizes = common_lab_ids.apply(len)

# Calculate the average and standard deviation
avg = sizes.mean()
std_dev = sizes.std()

print(f'Average: {avg}, Standard Deviation: {std_dev}')

# new stats portion end here 





# count the 1s and 0s in the inbox_res column
num_0 = merged_df['inbox_correct_to_2018'].value_counts()[0]
num_1 = merged_df['inbox_correct_to_2018'].value_counts()[1]
num_2 = merged_df['inbox_correct_to_2018'].value_counts()[2]
num_3 = merged_df['inbox_correct_to_2018'].value_counts()[3]
num_4 = merged_df['inbox_correct_to_2018'].value_counts().get(4, 0)

total_num = num_0 + num_1 + num_2 + num_3 + num_4
unambiguous_num = num_0 + num_1
print(f"INcorrect to 2018: {num_0} Correct to 2018: {num_1} neither + or -: {num_2} both + and -: {num_3} no comment: {num_4} UNambiguisous total {unambiguous_num} ambiguisous total {total_num - unambiguous_num} total: {total_num}")


print(chalk.yellow("only unambiguious values considered:"))
# remove entries w/ inbox_res > 1
merged_df = merged_df[merged_df['inbox_correct_to_2018'] < 2]
actual_labels = merged_df['human_pos_culture_status'] # this is the same as above
predicted_labels = merged_df['inbox_res']

conf_matrix = confusion_matrix(actual_labels, predicted_labels)
conf_matrix_df = pd.DataFrame(conf_matrix, columns=['Predicted Negative', 'Predicted Positive'], index=['Actual Negative', 'Actual Positive'])
print(conf_matrix_df)
print('Note: "actual" = 2018 labeled result, "predicted" =inbox')

# Calculating Sensitivity and Specificity
TP = conf_matrix[1, 1]  # True Positives
TN = conf_matrix[0, 0]  # True Negatives
FP = conf_matrix[0, 1]  # False Positives
FN = conf_matrix[1, 0]  # False Negatives

sensitivity = TP / (TP + FN)
specificity = TN / (TN + FP)
ppv = TP / (TP + FP)
npv = TN / (TN + FN)
f1_score = 2 * (ppv * sensitivity) / (ppv + sensitivity)

print(f"Sensitivity: {sensitivity:.3f}")
print(f"Specificity: {specificity:.3f}")
print(f"PPV (Positive Predictive Value): {ppv:.3f}")
print(f"NPV (Negative Predictive Value): {npv:.3f}")
print(f"F1 Score: {f1_score:.3f}")
