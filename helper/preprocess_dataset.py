import glob
import os
import random
import pandas as pd


def generate_dataset(adult_file, febrl_file):
    adult_dataset = pd.read_csv(adult_file)
    febrl_dataset = pd.read_csv(febrl_file)

    adult_dataset = adult_dataset.head(10000)
    febrl_dataset = febrl_dataset.drop(columns=['rec_id', 'date_of_birth', 'age', 'phone_number', 'blocking_number'])

    dataset_org = pd.concat([adult_dataset.reset_index(drop=True), febrl_dataset.reset_index(drop=True)], axis=1)
    dataset_org.to_csv('../dataset/dataset_preparation/dataset_org.csv', index=False)


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
    # change null to space_fill_values
    df.fillna(space_fill_values, inplace=True)
    # df.replace(' ', space_fill_values, inplace=True)
    # delete space in the beginning and end of string
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df['street_number'] = df['street_number'].astype(int)

    # output preprocessed result
    df.to_csv(output_file_path, index=False)


def split_csv(file_path, output_file_dir_path):
    """
    split dataset. dataset_A=dataset.head(3000), dataset_B=dataset.tail(3000), all_record_linkage=dataset[2000:3000]
    the intersection of dataset_A and dataset_B is all_record_linkage
    :param file_path:
    :param output_file_dir_path:
    """
    df = pd.read_csv(file_path)

    # split dataset
    dataset_A = df.head(5500)
    dataset_B = df.tail(5500)
    all_record_linkage = df[4500:5500]

    # add index for each row. a1, a2, a3, b1, b2, b3, b4, b5, ..
    dataset_A.insert(0, 'index', [str(i) + '_a' for i in range(1, 1 + len(dataset_A))])
    dataset_B.insert(0, 'index', [str(i) + '_b' for i in range(1, 1 + len(dataset_B))])

    dataset_A.to_csv(output_file_dir_path + 'dataset_A.csv', index=False)
    dataset_B.to_csv(output_file_dir_path + 'dataset_B.csv', index=False)
    df.to_csv(output_file_dir_path + 'dataset.csv', index=False)
    all_record_linkage.to_csv(output_file_dir_path + 'all_record_linkage.csv', index=False)


def sort_hierarchy_set_index(input_file_path, output_file_path):
    df = pd.read_csv(input_file_path, header=None)
    columns_order = list(reversed(df.columns))
    df_sorted = df.sort_values(by=columns_order)
    df_sorted.insert(0, 'ID', range(1, 1 + len(df_sorted)))
    df_sorted.to_csv(output_file_path, index=False, header=False)


def random_modify_data(file_path, all_cols_to_modify, portion_row_to_modify=0.2, num_col_to_modify=3):
    df = pd.read_csv(file_path)
    num_rows = df.shape[0]
    rows_to_modify = random.sample(range(num_rows), int(num_rows * portion_row_to_modify))
    for row_index in rows_to_modify:
        columns_to_modify = random.sample(list(all_cols_to_modify), num_col_to_modify)
        for column in columns_to_modify:
            value = str(df.at[row_index, column])
            if len(value) < 3:
                continue
            choice = random.choice([1, 2, 3])
            if choice == 1:
                # delete a random letter
                index_to_remove = random.randint(0, len(value) - 1)
                value = value[:index_to_remove] + value[index_to_remove + 1:]
            elif choice == 2:
                # swap two random letters next to each other
                if len(value) >= 2:
                    index_to_swap = random.randint(0, len(value) - 2)
                    value = value[:index_to_swap] + value[index_to_swap + 1] + value[index_to_swap] + value[
                                                                                                      index_to_swap + 2:]
            elif choice == 3:
                # add a random letter
                index_to_add = random.randint(0, len(value))
                value = value[:index_to_add] + chr(random.randint(97, 122)) + value[index_to_add:]
            df.at[row_index, column] = value
    df.to_csv(file_path, index=False)


if __name__ == '__main__':
    adult_file_path = '../dataset/dataset_preparation/adult.csv'
    febrl_file_path = '../dataset/dataset_preparation/febrl.csv'
    original_file_path = '../dataset/dataset_preparation/dataset_org.csv'
    preprocessed_file_path = '../dataset/dataset_preparation/preprocessed_dataset.csv'
    modified_file_path = '../dataset/dataset_preparation/modified_dataset.csv'
    output_file_dir_path = '../dataset/'

    generate_dataset(adult_file_path, febrl_file_path)
    preprocess_dataset(original_file_path, preprocessed_file_path)
    split_csv(preprocessed_file_path, output_file_dir_path)

    dataset_A_file_path = '../dataset/dataset_A.csv'
    dataset_B_file_path = '../dataset/dataset_B.csv'
    random_modify_data(dataset_A_file_path, ['given_name', 'surname', 'address_1', 'address_2', 'suburb', 'state'])
    random_modify_data(dataset_B_file_path, ['given_name', 'surname', 'address_1', 'address_2', 'suburb', 'state'])



    # original_hierarchy_file_dir_path = '../dataset/hierarchy/old/'
    # preprocessed_hierarchy_file_dir_path = '../dataset/hierarchy/'
    # files = glob.glob(os.path.join(original_hierarchy_file_dir_path, '*.csv'))
    # for file_path in files:
    #     file_name = os.path.basename(file_path)
    #     sort_hierarchy_set_index(file_path, preprocessed_hierarchy_file_dir_path + file_name)
