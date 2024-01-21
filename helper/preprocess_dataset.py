import glob
import os
import pandas as pd


def preprocess_dataset(file_path, output_file_path):
    df = pd.read_csv(file_path)

    # delete space in column name
    df.columns = df.columns.str.strip()

    df['postcode'] = df['postcode'].astype(str).str.zfill(4)

    # fill space(missing values)
    space_fill_values = {
        'given_name': 'nogivenname',
        'surname': 'nosurname',
        'street_number': '0',
        'address_1': 'noaddressone',
        'address_2': 'noaddresstwo',
        'suburb': 'nosuburb',
        'postcode': '0000',
        'state': 'nostate',
        'date_of_birth': '10000101'
    }
    df.replace(' ', space_fill_values, inplace=True)
    # delete space in the beginning and end of string
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # output preprocessed result
    df.to_csv(output_file_path, index=False)


def split_csv(file_path, output_file_dir_path):
    """
    split dataset to 5 parts. 1,2,3 for dataset_A, 3,4,5 for dataset_B, 3 for all_record_linkage
    the intersection of dataset_A and dataset_B is all_record_linkage
    :param file_path:
    :param output_file_dir_path:
    """
    df = pd.read_csv(file_path)
    total_rows = len(df)
    chunk_size = total_rows // 5

    # split dataset to 5 parts. 1,2,3,4,5
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]
    # combine 1,2,3 to dataset_A
    dataset_A = pd.concat(chunks[:3])
    # combine 3,4,5 to dataset_B
    dataset_B = pd.concat(chunks[2:])
    # the 3. part is all_record_linkage.csv
    all_record_linkage = pd.concat(chunks[3:4])

    dataset_A.to_csv(output_file_dir_path + 'dataset_A.csv', index=False)
    dataset_B.to_csv(output_file_dir_path + 'dataset_B.csv', index=False)
    all_record_linkage.to_csv(output_file_dir_path + 'all_record_linkage.csv', index=False)


def sort_hierarchy_set_index(input_file_path, output_file_path):
    df = pd.read_csv(input_file_path, header=None)
    columns_order = list(reversed(df.columns))
    df_sorted = df.sort_values(by=columns_order)
    df_sorted.insert(0, 'ID', range(1, 1 + len(df_sorted)))
    df_sorted.to_csv(output_file_path, index=False, header=False)


if __name__ == '__main__':
    original_file_path = '../dataset/dataset.csv'
    # preprocessed_file_path = '../dataset/preprocessed_dataset.csv'
    output_file_dir_path = '../dataset/'
    original_hierarchy_file_dir_path = '../dataset/hierarchy/old/'
    preprocessed_hierarchy_file_dir_path = '../dataset/hierarchy/'

    # preprocess_dataset(original_file_path, preprocessed_file_path)

    # split_csv(original_file_path, output_file_dir_path)

    files = glob.glob(os.path.join(original_hierarchy_file_dir_path, '*.csv'))
    for file_path in files:
        file_name = os.path.basename(file_path)
        sort_hierarchy_set_index(file_path, preprocessed_hierarchy_file_dir_path + file_name)