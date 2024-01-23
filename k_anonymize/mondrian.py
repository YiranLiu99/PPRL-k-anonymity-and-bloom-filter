# Multi-Dimensional Mondrian for k-anonymity
import glob
import os
import pandas as pd
import k_anonymize.hierarchy_tree as h_tree


def summarized(partition, dim, qi_list):
    """
    change the values of the quasi-identifiers columns to the range of the values in the partition
    :param partition: the data frame to be anonymized
    :param dim: the dimension to be summarized
    :param qi_list: the quasi-identifiers to be used
    :return: the anonymized data frame
    """
    for qi in qi_list:
        partition = partition.sort_values(by=qi)
        if partition[qi].iloc[0] != partition[qi].iloc[-1]:
            s = f"{partition[qi].iloc[0]}-{partition[qi].iloc[-1]}"
            partition[qi] = [s] * partition[qi].size
    return partition


def anonymize(partition, ranks, k, qi_list):
    """
    recursively calls itself on the two halves of data
    :param partition: the data frame to be anonymized
    :param ranks: the ranks of the quasi-identifiers
    :param k: the k value for k-anonymity
    :param qi_list: the quasi-identifiers to be used
    :return: the anonymized data frame
    """
    # get the dimension with the highest rank
    dim = ranks[0][0]

    partition = partition.sort_values(by=dim)
    si = partition[dim].count()
    mid = si // 2
    left_partition = partition[:mid]
    right_partition = partition[mid:]
    if len(left_partition) >= k and len(right_partition) >= k:
        return pd.concat([anonymize(left_partition, ranks, k, qi_list), anonymize(right_partition, ranks, k, qi_list)])
    return summarized(partition, dim, qi_list)


def mondrian(partition, qi_list, k):
    """
    Mondrian algorithm for k-anonymity.
    :param partition: the data frame to be anonymized
    :param qi_list: the quasi-identifiers to be used
    :param k: the k value for k-anonymity
    :return: anonymized DataFrame where each group of records with the same quasi-identifiers has at least k records.
    """
    # find which quasi-identifier has the most distinct values
    ranks = {}
    for qi in qi_list:
        ranks[qi] = len(partition[qi].unique())
    # sort the ranks in descending order
    ranks = [(key, value) for key, value in sorted(ranks.items(), key=lambda item: item[1], reverse=True)]
    # print(ranks)
    return anonymize(partition, ranks, k, qi_list)


def map_text_to_num(df, qi_list, hierarchy_tree_dict):
    """
    the data frame with text values mapped to leaf_id(number). It would help to anonymize using mondrian algorithm
    :param df: the data frame to be anonymized
    :param qi_list: the quasi-identifiers to be used
    :param hierarchy_tree_dict: the hierarchy tree dictionary
    :return: the data frame with text values mapped to leaf_id(number).
    """
    # Iterate over each column in quasi_identifiers
    for column in qi_list:
        # Get the hierarchy tree for the current column
        hierarchy_tree = hierarchy_tree_dict[column]
        # Create a mapping of values to leaf_id
        # for leaf_id, leaf in hierarchy_tree.leaf_id_dict.items():
        #     print(column, type(leaf_id))
        if isinstance(df[column].iloc[0], str):
            mapping = {leaf.value: leaf_id for leaf_id, leaf in hierarchy_tree.leaf_id_dict.items()}
        else:  # isinstance(df[column].iloc[0], int)
            mapping = {int(leaf.value): leaf_id for leaf_id, leaf in hierarchy_tree.leaf_id_dict.items()}
        # Replace the values in the current column with their corresponding leaf_id
        df[column] = df[column].map(mapping)
    return df


def map_num_to_text(df, qi_list, hierarchy_tree_dict):
    """
    the data frame with leaf_id(number) mapped to text values(original or generalized).
    :param df: the data frame to be anonymized
    :param qi_list: the quasi-identifiers to be used
    :param hierarchy_tree_dict: the hierarchy tree dictionary
    :return: the data frame with leaf_id(number) mapped to text values.
    """
    # Iterate over each column in quasi_identifiers
    for column in qi_list:  # time: O(m*n) = (m<<n) = O(n)
        # Get the hierarchy tree for the current column
        hierarchy_tree = hierarchy_tree_dict[column]
        # Create a mapping of leaf_id to values
        for i, value in df[column].items():
            value = str(value)
            if value.isdigit():  # single number. e.g. 17. time: O(1)
                leaf = hierarchy_tree.leaf_id_dict[value]
                df.at[i, column] = leaf.value
            elif isinstance(value, str) and '-' in value:  # interval. e.g. [9-16]. time: O(1)
                leaf1_id, leaf2_id = map(str, value.split('-'))  # value.strip('[]').split('-')
                common_ancestor = hierarchy_tree.find_common_ancestor(leaf1_id, leaf2_id)
                df.at[i, column] = common_ancestor.value
    return df


def check_k_anonymity(df, qi_list, k):
    """
    check if all partitions are k-anonymous
    :param qi_list: the quasi-identifiers to be used
    :param df: the data frame to be anonymized
    :param k: the k value for k-anonymity
    :return: True if all partitions are k-anonymous, False otherwise
    """
    partition_list = df.groupby(qi_list)  # pandas groupby() time: O(n*log(n))
    check_k_anonymity_flag = True
    for partition_key, partition in partition_list:
        if len(partition) < k:
            check_k_anonymity_flag = False
    return check_k_anonymity_flag


def run_anonymize(qi_list, sensitive_attributes, identifier, date_file, hierarchy_file_dir, k=5):
    # suppose n records(num of rows). k-anonymity. m quasi-identifiers. Calculate time complexity
    df = pd.read_csv(date_file)

    hierarchy_tree_dict = h_tree.build_all_hierarchy_tree(hierarchy_file_dir)

    df = map_text_to_num(df, qi_list, hierarchy_tree_dict)  # time: O(n*m) = (m<<n) = O(n)

    # calculation of ranks of the quasi-identifiers. time: O(n*m)
    # sort the ranks in descending order. time: O(m*log(m))
    # anonymize. Recursively calls itself on the two halves of the data. time: O(n*log(n))
    # summarized. time: O(n)
    # total time complexity of mondrian: O(n*m + m*log(m) + n*log(n) + n) = O(n*m + n*log(n)) = (m<<n) = O(n*log(n))
    df = mondrian(df, qi_list, k)

    if not check_k_anonymity(df, qi_list, k):  # time: O(n*log(n))
        raise Exception("Not all partitions are k-anonymous")

    df = map_num_to_text(df, qi_list, hierarchy_tree_dict)  # time: O(n*m) = (m<<n) = O(n)
    # total time complexity: O(n*log(n))

    return df


if __name__ == '__main__':
    print("Test mode: Running mondrian.py")
    # k = 5
    # date_file_path = '../dataset/dataset.csv'
    # anonymized_file_dir_path = '../dataset/anonymized/'
    # hierarchy_file_dir_path = '../dataset/hierarchy/'
    #
    # quasi_identifiers = ['sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass',
    #                      'occupation']
    # sensitive_attributes = ['salary-class']
    # identifier = ['ID', 'soc_sec_id', 'given_name', 'surname']
    #
    # data_frame = run_anonymize(quasi_identifiers, sensitive_attributes, identifier, k, date_file_path,
    #                            anonymized_file_dir_path, hierarchy_file_dir_path)


