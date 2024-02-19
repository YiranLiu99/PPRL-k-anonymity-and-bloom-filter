import time
import pandas as pd
import recordlinkage as rl
from recordlinkage.base import BaseIndexAlgorithm
from recordlinkage.index import Block
import k_anonymize.hierarchy_tree as h_tree


def find_candidate_links(partitions_A, partitions_B, hierarchy_trees, qi_list):
    """
    For each column in quasi_identifier, check the values(value_a, value_b) in df_a and df_b at this column,
    if value_a == value_b, then link value_a and value_b as candidate link.
    Find the nodes(node_a, node_b) in hierarchy_tree which value_a and value_b belong to,
    if node_a is covered by node_b(node_a in node_b's subtree)
    or node_b is covered by node_a(node_b in node_a's subtree),
    then link node_a and node_b as candidate link.
    :param partitions_A: key is the index of first record in each partition, value is the partition.
    :param partitions_A:
    :param hierarchy_trees:
    :param qi_list:
    :return: candidate_links
    """
    # Time complexity:
    # k anonymity. In total n records in datasetA and n records in datasetB.
    # n/k partitions in datasetA and n/k partitions in datasetB.
    # n/k records in data_frame_a and n/k records in data_frame_b
    # q quasi-identifiers in qi_list,
    # h is the height of the hierarchy tree.
    # h, q, k << n
    # O(n/k * n/k * q * h) = O(n^2/k^2)
    candidate_links = []
    for index_a, partition_a in partitions_A.items():
        for index_b, partition_b in partitions_B.items():
            link = True
            # Use the first record of each partition for comparison because the records in same partition are the totally same.
            row_a = partition_a.iloc[0]
            row_b = partition_b.iloc[0]
            for attribute in qi_list:
                value_a = str(row_a[attribute])
                value_b = str(row_b[attribute])
                if not (hierarchy_trees[attribute].check_node_covered(value_a, value_b) or
                        hierarchy_trees[attribute].check_node_covered(value_b, value_a) or
                        value_a == value_b):
                    link = False
                    break
            if link:
                # If a link is found, add a candidate link for each record in the partitions
                for record_a in partition_a.index:
                    for record_b in partition_b.index:
                        candidate_links.append((record_a, record_b))
    return pd.MultiIndex.from_tuples(candidate_links, names=['index_a', 'index_b'])
    # return candidate_links


def split_data_to_partitions(df, qi_list):
    """
    Split the dataframe into partitions. Each records in each partition are the totally same. (Because of k-anonymity)
    And extract the first record of each partition as the representative record of this partition.
    :param df: the dataframe to be split.
    :param qi_list: the quasi-identifiers to be used.
    :return: a dict of partitions, and a list of representative records.
    """
    partition_list = df.groupby(qi_list)
    # partition_list to partition_dict: keys are the index of first record in each partition, values are the partition.
    partition_dict = {}
    for partition_key, partition in partition_list:
        # get the index of the first record in this partition.
        partition_dict[partition.iloc[0].name] = partition
    return partition_dict


def block_data(anonymized_data_path_A, anonymized_data_path_B, hierarchy_file_dir, qi_list):
    """
    Block the data. Find candidate links.
    :param anonymized_data_path_A:
    :param anonymized_data_path_B:
    :param hierarchy_file_dir:
    :param qi_list:
    :return: candidate_links, candidate_record_set_A, candidate_record_set_B
    """
    start_time = time.time()
    print(f'Start finding candidate links for {anonymized_data_path_A} and {anonymized_data_path_B}')
    df_a = pd.read_csv(anonymized_data_path_A, index_col='index')
    df_b = pd.read_csv(anonymized_data_path_B, index_col='index')

    # split df_a and df_b into partitions. partition_dict_A and partition_dict_B are the dict of partitions.
    # Since each record in each partition are the totally same,
    # we can extract the first record of each partition as the representative record of this partition.
    # in this way, we can reduce the number of records to be compared.
    partition_dict_A = split_data_to_partitions(df_a, qi_list)
    partition_dict_B = split_data_to_partitions(df_b, qi_list)
    print(f'{len(partition_dict_A)} partitions in dataset A')
    print(f'{len(partition_dict_B)} partitions in dataset B')

    # Find candidate links.
    # If two records have the same value in each attribute, or have a covered relationship in each attribute, then they are candidate link.
    # If two records don't have the same value in each attribute, and don't have a covered relationship in each attribute, then they are not candidate link.
    # e.g. if record_A's age is [21-30], record_B's age is [21-30], then record_A and record_B are same at attribute age.
    # e.g. if record_A's marital-status is 'spouse present', record_B's marital-status is 'spouse present', then record_A and record_B are same at attribute marital-status.
    # e.g. if record_A's age is [21-30], record_B's age is [21-25], then record_A and record_B have a covered relationship at attribute age.
    # e.g. if record_A's education is Professional Education, record_B's education is higher education, then record_A and record_B have a covered relationship at attribute education.
    # e.g. if record_A's age is 17, record_B's age is [21-25], then record_A and record_B don't have a covered relationship.
    hierarchy_tree_dict = h_tree.build_all_hierarchy_tree(hierarchy_file_dir)
    candidate_links = find_candidate_links(partition_dict_A, partition_dict_B, hierarchy_tree_dict,
                                           qi_list)  # O(n^2/k^2)
    candidate_record_set_A = set(candidate_links.get_level_values('index_a'))
    candidate_record_set_B = set(candidate_links.get_level_values('index_b'))

    print(f'Candidate links found for {anonymized_data_path_A} and {anonymized_data_path_B}')
    print(candidate_links)
    # print(candidate_record_set_A)
    # print(candidate_record_set_B)
    print(f'{len(candidate_record_set_A)} candidate records for dataset A')
    print(f'{len(candidate_record_set_B)} candidate records for dataset B')

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Run time: " + str(elapsed_time) + " seconds")
    return candidate_links, candidate_record_set_A, candidate_record_set_B


if __name__ == '__main__':
    print("Test mode: Running mondrian.py")

    # quasi_identifiers = ['sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass',
    #                      'occupation']
    # df_a = pd.read_csv('../dataset/dataset_A/k_5_anonymized_datasetA_no_sa_ident.csv', index_col='index')
    # df_b = pd.read_csv('../dataset/dataset_B/k_5_anonymized_datasetB_no_sa_ident.csv', index_col='index')
    # hierarchy_file_dir = '../dataset/hierarchy/'
    #
    # partition_dict_A = split_data_to_partitions(df_a, quasi_identifiers)
    # partition_dict_B = split_data_to_partitions(df_b, quasi_identifiers)
    # print(len(partition_dict_A))
    # print(len(partition_dict_B))
    #

    # hierarchy_tree_dict = h_tree.build_all_hierarchy_tree(hierarchy_file_dir)
    # candidate_links = find_candidate_links(partition_dict_A, partition_dict_B, hierarchy_tree_dict,
    #                                        quasi_identifiers)  # O(n^2/k^2)
    # candidate_record_set_A = set(candidate_links.get_level_values('index_a'))
    # candidate_record_set_B = set(candidate_links.get_level_values('index_b'))
    #
    # print(candidate_links)
    # print(candidate_record_set_A)
    # print(candidate_record_set_B)
    # print(len(candidate_record_set_A))
    # print(len(candidate_record_set_B))


