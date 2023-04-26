import re
import pandas as pd
import ast
import logging
import sys

import nltk
# nltk.download('punkt')
# nltk.download('stopwords')

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

def camel_case_to_words(word):
    """
    Camel case 형식의 문자열을 띄어쓰기로 분리하여 반환
    """
    # 대문자 앞에 공백을 추가하고 소문자로 변경하여 문자열을 재구성
    word = re.sub(r'([A-Z][a-z]+)', r' \1', word)
    # 모든 대문자를 소문자로 변경
    word = word.lower()
    # 문자열 양 끝의 공백 제거
    word = word.strip()
    
    return word


def tokenize(sentence):
    pre_process_template = []
    
    # 문자 아닌 것 제거
    sentence = re.sub(r'[^\w\s]', ' ', sentence) 
    # 토큰화
    tokens = word_tokenize(sentence) 
    # print(tokens)

    stop_words = stopwords.words('english')

    # stop word 제거
    filtered_tokens = [word for word in tokens if word.lower() not in stop_words] 
    # print(filtered_tokens)

    # camel case 처리
    for token in filtered_tokens:
        words = camel_case_to_words(token)
        # print(words.split())
        pre_process_template += words.split()
    
    return pre_process_template


def preprocessing_HDFS(data_file_path):
    parsed_df = pd.read_csv(data_file_path)
    parsed_df['LogSequence'] = parsed_df['LogSequence'].apply(lambda x: ast.literal_eval(x))
    
    line_count = 0
    seq = 0
    
    logger.info(f'Preprocessing file {data_file_path}')
    
    for idx, row in parsed_df.iterrows():
        template_list = []
        
        for line in row['LogSequence']:
            pre_process_template = tokenize(line)
            # print(pre_process_template)
            template_list.append(pre_process_template)
            line_count += 1
        
        seq += 1
        parsed_df.at[idx, 'LogSequence'] = template_list
        # logger.info(f"BlockID {row['BlockId']}: Log events {len(row['LogSequence'])}, Success {len(template_list)}")
    
    logger.info(f'--- Done pre-processing of Log Events: Total log sequences {seq}, Total log events {line_count}')
    print(parsed_df)
    return parsed_df


if __name__=='__main__':
    df = preprocessing_HDFS('data/template_HDFS_2k_sequence.csv')
    df.to_csv('data/processed_HDFS_2k.csv', index=False)