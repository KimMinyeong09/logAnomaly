import os
import pandas as pd
import sys
import ast
import re

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from drain3.template_miner_config import TemplateMinerConfig
from drain3 import TemplateMiner
from dataload import dataloader

def replace_angle_brackets(pattern_str, string):
    # <::> 형식을  * 로 변환
    pattern = re.compile(pattern_str)
    matches = re.findall(pattern, string)

    for match in matches:
        string = string.replace(f'/<:{match}:>:', '*')
        string = string.replace(f'/<:{match}:>', '*')
        string = string.replace(f'<:{match}:>/', '*')
        string = string.replace(f':<:{match}:>', '*')
        string = string.replace(f'<:{match}:>:', '*')
        string = string.replace(f'<:{match}:>', '*')
    string = string.replace('**', '*')
    string = string.replace('blk *', '*')

    return string


def set_template_miner():
    config = TemplateMinerConfig()
    config.load(os.path.dirname(__file__) + "/drain3.ini")
    config.profiling_enabled = True
    template_miner = TemplateMiner(config=config)
    return template_miner


def parsing_HDFS(log_file):
    dataloader.check_file_existence(log_file)
    
    template_miner = set_template_miner()
    
    line_count = 0
    
    labeled_df = pd.read_csv(log_file)
    labeled_df['LogSequence'] = labeled_df['LogSequence'].apply(lambda x: ast.literal_eval(x))
    
    for idx, row in labeled_df.iterrows():
        template_list = []
        
        for line in row['LogSequence']:
            result = template_miner.add_log_message(line)
            line_count += 1
            template_mined = result['template_mined']
            template = replace_angle_brackets('<:(.*?):>', template_mined)
            template_list.append(template)
            
        labeled_df.at[idx, 'LogSequence'] = template_list

    print(labeled_df)
    return labeled_df
        
if __name__=='__main__':
    # 테스트용
    # labeled_file = 'data\labeled_HDFS_2k_sequence.csv'
    # template_file = 'template_HDFS_2k_sequence.csv'
    # 실제 데이터
    labeled_file = 'data\labeled_HDFS_sequence.csv'
    template_file = 'template_HDFS_sequence.csv'
    
    # 라벨링 되어 있는 시퀀스 데이터들을 파싱하여 템플릿으로 처리
    template_df = parsing_HDFS(labeled_file)
    template_df.to_csv('data/'+ template_file, index=False)
    