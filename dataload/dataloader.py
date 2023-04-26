import csv
import os
import pandas as pd
from collections import OrderedDict
import re


def check_file_existence(file_path):
    if file_path is None:
        print('Error:: file_path is None ')
        return False
    
    if not os.path.exists(file_path):
        print(f'Error:: A file dose not exist: {file_path}')
        return False
    return True

def get_log_to_csv(log_file_path=None, save_csv_file_name='logData'):
    # save log file as csv file
    
    if not check_file_existence(log_file_path):
        return 
    
    in_log_file = log_file_path
    line_count = 0

    with open(in_log_file) as f:
        lines = f.readlines()

    with open(save_csv_file_name + '.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Content'])
        for line in lines:
            line = line.rstrip()
            line = line.partition(": ")[2]
            # print(line)
            writer.writerow([line])
            line_count += 1
        print(f'---Saving log data as CSV is completed: lines {line_count}, csv_file {save_csv_file_name}')
            
            
def load_HDFS(log_file = None):
    '''
    Load HDFS log data
    
    Arguments
    ---------
        log_file : The file extension of the file is csv.
    
    Return
    ------
        data_df : Dataframe that distinguishes log sequences by dividing sessions by block IDs.
    '''
    
    if not check_file_existence(log_file):
        return 
    
    if log_file.endswith(".csv"):
        print(f'Loading {log_file}...')
                
        struct_log = pd.read_csv(log_file, engine='c', na_filter=False, memory_map=True)
        data_dict = OrderedDict()

        for idx, row in struct_log.iterrows():
            blkId_list = re.findall(r'(blk_-?\d+)', row['Content'])
            # print(blkId_list)
            blkId_set = set(blkId_list)
            for blk_Id in blkId_set:
                if not blk_Id in data_dict:
                    data_dict[blk_Id] = []
                data_dict[blk_Id].append(row['Content'])
        data_df = pd.DataFrame(list(data_dict.items()), columns=['BlockId', 'LogSequence'])
        
        print(f'---Finish loading {log_file}')
        return data_df

    else:
        print(f'Error:: This is not a CSV file: {log_file}')
        return


def labeling_HDFS(original_df):
    '''
    Label HDFS log data
    
    Arguments
    ---------
        original_data : The pandas dataframe obtained by using 'load_HDFS' function.
    
    Return
    ------
        labled_df : Dataframe containing labeled data based on 'anomaly_labeled.csv' for original_df
    '''
    if not isinstance(original_df, pd.DataFrame):
        print(f"Error:: Input parameter {original_df} must be a pandas dataframe")
        return
    
    labeled_df = pd.read_csv('data/anomaly_label.csv')
    labeled_df = pd.merge(labeled_df, original_df, on='BlockId')
    
    print('---Finish labeling data set')
    return labeled_df

if __name__=='__main__':
    # 기존 로그 파일을 csv로 변환
    # get_log_to_csv(log_file_path='logParsing\HDFS_2k.log', save_csv_file_name='data\HDFS_2k')
    
    # 블록ID로 로그 시퀀스 구한 데이터프레임 반환
    data_df = load_HDFS('data/HDFS_2k.csv')
    labeled_df = labeling_HDFS(data_df)
    data_df.to_csv('data/HDFS_2k_sequence.csv', index=False)
    labeled_df.to_csv('data/labeled_HDFS_2k_sequence.csv', index=False)