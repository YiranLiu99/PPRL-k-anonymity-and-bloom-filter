# Multi-Dimensional Mondrian for k-anonymity
import glob
import os

import pandas as pd
import k_anonymize.hierarchy_tree as h_tree


def summarized(partition, dim):
    """
    :param partition: the data frame to be anonymized
    :param dim: the dimension to be summarized
    :return: the anonymized data frame
    """
    partition = partition.sort_values(by='age')
    if partition['age'].iloc[0] != partition['age'].iloc[-1]:
        s = f"[{partition['age'].iloc[0]}-{partition['age'].iloc[-1]}]"
        partition['age'] = [s] * partition['age'].size
    return partition

def anonymize(partition, ranks, k):
    """
    :param partition: the data frame to be anonymized
    :param ranks: the ranks of the quasi-identifiers
    :param k: the k value for k-anonymity
    :return: the anonymized data frame
    """
    # get the dimension with the highest rank
    dim = ranks[0][0]

    partition = partition.sort_values(by=dim)
    si = partition[dim].count()
    mid = si // 2
    left_partition = partition[:mid]
    right_partition = partition[mid:]
    if (len(left_partition) >= k and len(right_partition) >= k):
        return pd.concat([anonymize(left_partition, ranks, k), anonymize(right_partition, ranks, k)])
    return summarized(partition, dim)


def mondrian(partition, quasi_identifiers, k):
    """
    :param partition: the data frame to be anonymized
    :param quasi_identifiers: the quasi-identifiers to be used
    :param k: the k value for k-anonymity
    :return: the anonymized data frame
    """
    # find which quasi-identifier has the most distinct values
    ranks = {}
    for qi in quasi_identifiers:
        ranks[qi] = len(partition[qi].unique())
    # sort the ranks in descending order
    ranks = [(k, v) for k, v in sorted(ranks.items(), key=lambda item: item[1], reverse=True)]
    print(ranks)
    return anonymize(partition, ranks, k)




if __name__ == '__main__':
    # date_file_path = '../dataset/dataset.csv'
    # df = pd.read_csv(date_file_path)
    #
    # quasi_identifiers = ['sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass',
    #                      'occupation']
    #
    # data = mondrian(df, quasi_identifiers, 5)
    # data.to_csv('../dataset/mondrian.csv', index=False)

    hierarchy_file_dir_path = '../dataset/hierarchy/'
    hierarchy_tree_dict = h_tree.build_all_hierarchy_tree(hierarchy_file_dir_path)

    print(hierarchy_tree_dict['education'].node_dict)

