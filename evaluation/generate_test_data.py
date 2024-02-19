import os
import pandas as pd
import recordlinkage as rl
from recordlinkage.index import Full
from helper.preprocess_dataset import random_modify_data
from k_anonymize import mondrian
from record_linkage.bloom import encode_bloom
from run import participant


def generate_dataset_diff_size(file_path, output_file_dir_path, intersection_size, dataset_size):
    """

    :param file_path:
    :param output_file_dir_path:
    :param intersection_size:
    :param dataset_size:
    :return:
    """
    df = pd.read_csv(file_path)
    dataset_A = df.head(dataset_size)  # record 0~(dataset_size-1)
    record_linkage_set = dataset_A.tail(intersection_size)  # record (dataset_size-intersection_size)~(dataset_size-1)
    dataset_B = pd.concat([record_linkage_set, df.iloc[dataset_size:(dataset_size+dataset_size-intersection_size)]]).reset_index(drop=True)

    # add index for each row. a1, a2, a3, b1, b2, b3, b4, b5, ..
    dataset_A.insert(0, 'index', [str(i) + '_a' for i in range(1, 1 + len(dataset_A))])
    dataset_B.insert(0, 'index', [str(i) + '_b' for i in range(1, 1 + len(dataset_B))])

    dataset_A.to_csv(output_file_dir_path + 'dataset_A.csv', index=False)
    dataset_B.to_csv(output_file_dir_path + 'dataset_B.csv', index=False)
    record_linkage_set.to_csv(output_file_dir_path + 'all_record_linkage.csv', index=False)


def generate_dataset_diff_k(qi_list, sa_list, ident_list, original_data_path, hierarchy_file_dir_path, k, anonymized_data_path):
    df = mondrian.run_anonymize(qi_list, sa_list, ident_list, original_data_path, hierarchy_file_dir_path, k)
    df.to_csv(anonymized_data_path, index=False)
    df = pd.read_csv(anonymized_data_path)
    df.drop(columns=ident_list, inplace=True)
    df.drop(columns=sa_list, inplace=True)
    df.drop(columns=['ID'], inplace=True)
    df.to_csv(anonymized_data_path, index=False)


def encode_identifiers_for_diff_data_size(org_file_path, encoded_file_path, num_hash=10, num_bits=1000):
    df = pd.read_csv(org_file_path)
    df['address_1_num'] = df['address_1'] + df['street_number'].astype(str)
    df['state_postcode'] = df['state'] + df['postcode'].astype(str)
    df = df.drop(['street_number', 'address_1', 'postcode', 'state', 'soc_sec_id'], axis=1)
    df = df.drop(['ID', 'sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass', 'occupation', 'salary-class'], axis=1)

    # encode identifiers into bloom filters
    for i, col in enumerate(df.columns[1:]):
        df[col] = df[col].astype(str).apply(lambda x: encode_bloom(x, num_bits, num_hash))
    # save encoded identifiers to compressed csv file using zip
    df.to_csv(encoded_file_path, index=False, compression='zip')


def generate_all_record_pairs(data_path_A, data_path_B, all_record_pairs_path):
    """
    Generate all record pairs from two datasets without any anonymization or blocking.
    :param data_path_A:
    :param data_path_B:
    :param all_record_pairs_path:
    :return:
    """
    df1 = pd.read_csv(data_path_A, index_col="index")
    df2 = pd.read_csv(data_path_B, index_col="index")
    indexer = rl.Index()
    indexer.add(Full())
    pairs = indexer.index(df1, df2)
    all_index = pd.MultiIndex.from_tuples(pairs)
    df = pd.DataFrame(index=all_index).reset_index()
    print(df)
    df.to_csv(all_record_pairs_path, index=False, compression='zip')


def find_candidate_links_for_change_k(anonymized_path_A, anonymized_path_B, candidate_links_dir_path, hierarchies_dir_path):
    qs_list = ['sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass', 'occupation']
    classifier1 = participant.Classifier1(anonymized_path_A, anonymized_path_B, candidate_links_dir_path, hierarchies_dir_path, qs_list)
    candidate_links_path, candidate_record_set_path_A, candidate_record_set_path_B = classifier1.send_candidate_links()


def identify_link(encoded_identifiers_file_path_A, encoded_identifiers_file_path_B, candidate_links_path, compared_links_file_path, matched_links_file_path, threshold=0.8):
    classifier2 = participant.Classifier2(encoded_identifiers_file_path_A, encoded_identifiers_file_path_B,
                                          candidate_links_path, compared_links_file_path, matched_links_file_path,
                                          threshold=threshold)
    classifier2.compare_links()
    classifier2.identify_record_linkage()


if __name__ == '__main__':
    # 1. Generate test data for change_data_size
    # size(dataset_A)=size(dataset_B)=1000, intersection=size(record_linkage)=200
    # size(dataset_A)=size(dataset_B)=1500, intersection=size(record_linkage)=200
    # size(dataset_A)=size(dataset_B)=2000, intersection=size(record_linkage)=400
    # size(dataset_A)=size(dataset_B)=2500, intersection=size(record_linkage)=400
    # size(dataset_A)=size(dataset_B)=3000, intersection=size(record_linkage)=600
    # size(dataset_A)=size(dataset_B)=3500, intersection=size(record_linkage)=600
    # size(dataset_A)=size(dataset_B)=4000, intersection=size(record_linkage)=800
    # size(dataset_A)=size(dataset_B)=4500, intersection=size(record_linkage)=800
    # size(dataset_A)=size(dataset_B)=5000, intersection=size(record_linkage)=1000
    # size(dataset_A)=size(dataset_B)=5500, intersection=size(record_linkage)=1000
    cols_to_modify = ['given_name', 'surname', 'address_1', 'address_2', 'suburb', 'state']
    data_sizes = [(1500,200), (2500,400), (3500,600), (4500,800)]
    # for size in data_sizes:
    #     datasize = size[0]
    #     intersection_size = size[1]
    #     print(f'size={datasize}')
    #     output_dir = f'test_dataset/change_data_size/data_size_{datasize}/'
    #     generate_dataset_diff_size('../dataset/dataset.csv', output_dir, intersection_size, datasize)
    #     random_modify_data(output_dir + 'dataset_A.csv', cols_to_modify, portion_row_to_modify=0.2, num_col_to_modify=3)
    #     random_modify_data(output_dir + 'dataset_B.csv', cols_to_modify, portion_row_to_modify=0.2, num_col_to_modify=3)

    # 2. Generate test data for change_k
    # k=5,10,15,20
    quasi_identifiers = ['sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass', 'occupation']
    sensitive_attributes = ['salary-class']
    identifier = ['given_name', 'surname', 'street_number', 'address_1', 'address_2', 'suburb', 'postcode', 'state', 'soc_sec_id']
    original_data_path_A = 'test_dataset/change_k/dataset_A.csv'
    original_data_path_B = 'test_dataset/change_k/dataset_B.csv'
    hierarchy_file_dir_path = '../dataset/hierarchy/'
    # for k in range(5, 21, 5):
    #     print(f'k={k}')
    #     generate_dataset_diff_k(quasi_identifiers, sensitive_attributes, identifier, original_data_path_A, hierarchy_file_dir_path,
    #                             k, f'test_dataset/change_k/k_{k}/k_{k}_anonymized_dataset_A_no_sa_ident.csv')
    #     generate_dataset_diff_k(quasi_identifiers, sensitive_attributes, identifier, original_data_path_B, hierarchy_file_dir_path,
    #                             k, f'test_dataset/change_k/k_{k}/k_{k}_anonymized_dataset_B_no_sa_ident.csv')


    # # 3. Find candidate links for change_k
    # find_candidate_links_for_change_k('test_dataset/change_num_hash/k_5_anonymized_dataset_A_no_sa_ident.csv',
    #                                       'test_dataset/change_num_hash/k_5_anonymized_dataset_B_no_sa_ident.csv',
    #                                       'test_dataset/change_num_hash/', hierarchy_file_dir_path)
    #
    # # 4. Identify links for change_k
    # encoded_identifiers_file_path_A = f'test_dataset/change_k/encoded_identifiers_A.zip'
    # encoded_identifiers_file_path_B = f'test_dataset/change_k/encoded_identifiers_B.zip'
    # for k in range(1, 22, 2):
    #     print(f'k={k}')
    #     candidate_links_path = f'test_dataset/change_k/k_{k}/candidate_links.zip'
    #     matched_links_file_path = f'test_dataset/change_k/k_{k}/matched_links.csv'
    #     identify_links_for_change_k(encoded_identifiers_file_path_A, encoded_identifiers_file_path_B, candidate_links_path, matched_links_file_path)

    # # 5.Encode identifiers for different data size
    # for size in data_sizes:
    #     datasize = size[0]
    #     print(f'Encoding for size={datasize}')
    #     encode_identifiers_for_diff_data_size(f'test_dataset/change_data_size/data_size_{datasize}/dataset_A.csv',
    #                                           f'test_dataset/encoded_data/data_size_{datasize}/encoded_identifiers_A.zip')
    #     encode_identifiers_for_diff_data_size(f'test_dataset/change_data_size/data_size_{datasize}/dataset_B.csv',
    #                                           f'test_dataset/encoded_data/data_size_{datasize}/encoded_identifiers_B.zip')



    # # generate_all_record_pairs('test_dataset/change_k/dataset_A.csv', 'test_dataset/change_k/dataset_B.csv',
    # #                           'test_dataset/change_k/no_anonymization/candidate_links.zip')


    datasize = 4500
    for threshold in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]:
        print(f'datasize={datasize}')
        print(f'threshold={threshold}')
        # encoded_identifiers_file_path_A = f'test_dataset/change_k/encoded_identifiers_A.zip'
        # encoded_identifiers_file_path_B = f'test_dataset/change_k/encoded_identifiers_B.zip'
        # candidate_links_path = f'test_dataset/change_num_hash/candidate_links.zip'
        compared_links_file_path = f'test_dataset/change_data_size/data_size_{datasize}/no_anonymization/compared_links.zip'
        matched_links_file_path = f'test_dataset/change_data_size/data_size_{datasize}/no_anonymization/matched_links_differ_threshold/matched_links_threshold_{int(threshold*10)}.csv'
        # classifier2 = participant.Classifier2(encoded_identifiers_file_path_A, encoded_identifiers_file_path_B,
        #                                       candidate_links_path, compared_links_file_path, matched_links_file_path,
        #                                       threshold=0.8)
        # classifier2.compare_links()
        df_compare = pd.read_csv(compared_links_file_path, index_col=[0, 1])
        df_matched = df_compare[(df_compare.T >= threshold).all()]
        print(len(df_matched))
        df_matched.to_csv(matched_links_file_path)




