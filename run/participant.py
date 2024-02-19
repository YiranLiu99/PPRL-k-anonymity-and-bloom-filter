import csv
import os
import numpy as np
import pandas as pd
import recordlinkage
from recordlinkage.base import BaseCompareFeature
from k_anonymize import mondrian
from record_linkage.block_links import block_data
from record_linkage.bloom import encode_bloom


class DataHolder:
    def __init__(self, holder_name, original_data_path, anonymized_data_dir_path, hierarchy_file_dir_path,
                 quasi_identifiers, sensitive_attributes, identifier, k=5):
        self.holder_name = holder_name
        self.original_data_path = original_data_path
        self.anonymized_data_dir_path = anonymized_data_dir_path
        self.anonymized_data_path = f'{self.anonymized_data_dir_path}k_{k}_anonymized_dataset_{self.holder_name}.csv'
        self.anonymized_data_no_sa_ident_path = f'{self.anonymized_data_dir_path}k_{k}_anonymized_dataset_{self.holder_name}_no_sa_ident.csv'
        self.candidate_records_index_file_path = f'{self.anonymized_data_dir_path}candidate_records_index_{self.holder_name}.csv'
        self.encoded_identifiers_file_path = f'{self.anonymized_data_dir_path}encoded_identifiers_{self.holder_name}.zip'
        self.hierarchy_file_dir_path = hierarchy_file_dir_path
        self.quasi_identifiers = quasi_identifiers
        self.sensitive_attributes = sensitive_attributes
        self.identifier = identifier
        self.k = k

    def get_original_data(self):
        return self.original_data_path

    def get_anonymized_data(self):
        return self.anonymized_data_path

    def get_anonymized_data_no_sa_ident(self):
        return self.anonymized_data_no_sa_ident_path

    def get_encoded_identifiers_file_path(self):
        return self.encoded_identifiers_file_path

    def anonymize_data_and_save(self):
        print(f'Anonymizing data for dataholder {self.holder_name}...')
        df = mondrian.run_anonymize(self.quasi_identifiers, self.sensitive_attributes, self.identifier,
                                    self.original_data_path, self.hierarchy_file_dir_path, self.k)
        df.to_csv(self.anonymized_data_path, index=False)
        print(f'Anonymize data for dataholder {self.holder_name} successfully! Saved at {self.anonymized_data_path}')

    def remove_sensitive_attributes_and_identifiers(self):
        df = pd.read_csv(self.anonymized_data_path)
        df.drop(columns=self.identifier, inplace=True)
        df.drop(columns=self.sensitive_attributes, inplace=True)
        df.drop(columns=['ID'], inplace=True)
        df.to_csv(self.anonymized_data_no_sa_ident_path, index=False)
        print(f'Remove sensitive attributes and identifiers for dataholder {self.holder_name} successfully! '
              f'Saved at {self.anonymized_data_no_sa_ident_path}')

    def send_anonymized_data(self):
        self.anonymize_data_and_save()
        self.remove_sensitive_attributes_and_identifiers()
        return self.anonymized_data_no_sa_ident_path

    def send_encode_identifiers_in_bloom_filter(self):
        df_r_index = pd.read_csv(self.candidate_records_index_file_path, header=None, names=['index'])
        df_dataset = pd.read_csv(self.original_data_path)
        # find the intersection of df_r_index and df_dataset using index
        df_merge = pd.merge(df_r_index, df_dataset, on='index', how='left')
        df_merge = df_merge[['index', 'given_name', 'surname', 'street_number', 'address_1', 'address_2', 'suburb', 'postcode', 'state']]
        df_merge['address_1_num'] = df_merge['address_1'] + df_merge['street_number'].astype(str)
        df_merge['state_postcode'] = df_merge['state'] + df_merge['postcode'].astype(str)
        df_merge = df_merge.drop(['street_number', 'address_1', 'postcode', 'state'], axis=1)

        # encode identifiers into bloom filters
        for i, col in enumerate(df_merge.columns[1:]):
            df_merge[col] = df_merge[col].astype(str).apply(lambda x: encode_bloom(x, 500, 10))
            # print(f'Encoding {i}th column {col} successfully!')
        # save encoded identifiers to compressed csv file using zip
        df_merge.to_csv(self.encoded_identifiers_file_path, index=False, compression='zip')
        print(f'Encode identifiers for dataholder {self.holder_name} successfully! Saved at {self.encoded_identifiers_file_path}')
        return self.encoded_identifiers_file_path


class Classifier1:
    def __init__(self, anonymized_data_path_A, anonymized_data_path_B, classifier_data_dir_path, hierarchy_file_dir_path, qs_list):
        self.anonymized_data_path_A = anonymized_data_path_A
        self.anonymized_data_path_B = anonymized_data_path_B
        self.classifier_data_dir_path = classifier_data_dir_path
        self.hierarchy_file_dir_path = hierarchy_file_dir_path
        self.qs_list = qs_list

    def send_candidate_links(self):
        (candidate_links,
         candidate_record_set_A,
         candidate_record_set_B) = block_data(self.anonymized_data_path_A,
                                              self.anonymized_data_path_B,
                                              self.hierarchy_file_dir_path,
                                              self.qs_list)
        candidate_records_index_file_path_A = f'{os.path.dirname(self.anonymized_data_path_A)}/candidate_records_index_A.csv'
        candidate_records_index_file_path_B = f'{os.path.dirname(self.anonymized_data_path_B)}/candidate_records_index_B.csv'
        # save candidate links to csv file at classifier_data_dir_path
        all_index = pd.MultiIndex.from_tuples(candidate_links)
        df = pd.DataFrame(index=all_index).reset_index()
        candidate_links_file_path = f'{self.classifier_data_dir_path}candidate_links.zip'
        # set the header as index_A and index_B
        df.columns = ['index_A', 'index_B']
        df.to_csv(candidate_links_file_path, index=False, compression='zip')
        print(f'Find candidate links to successfully! Saved at {candidate_links_file_path}')
        # save candidate record set A to csv file at dataset_A
        with open(candidate_records_index_file_path_A, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # csv_writer.writerow(['index'])
            for element in candidate_record_set_A:
                csv_writer.writerow([element])
        print(f'Candidate records for A saved at {candidate_records_index_file_path_A}')
        # save candidate record set B to csv file at dataset_B
        with open(candidate_records_index_file_path_B, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            # csv_writer.writerow(['index'])
            for element in candidate_record_set_B:
                csv_writer.writerow([element])
        print(f'Candidate records for B saved at {candidate_records_index_file_path_B}')
        return candidate_links_file_path, candidate_records_index_file_path_A, candidate_records_index_file_path_B


class Classifier2:
    def __init__(self, encoded_identifiers_file_path_A, encoded_identifiers_file_path_B, candidate_links_file_path, compared_links_file_path, matched_links_file_path, threshold=0.8):
        self.encoded_identifiers_file_path_A = encoded_identifiers_file_path_A
        self.encoded_identifiers_file_path_B = encoded_identifiers_file_path_B
        self.candidate_links_file_path = candidate_links_file_path
        self.compared_links_file_path = compared_links_file_path
        self.matched_links_file_path = matched_links_file_path
        self.threshold = threshold

    def compare_bloom_filter(self, bit_seq_A, bit_seq_B):
        """
        Compare two bloom filters using Dice-coefficient
        :param bit_seq_A: bit sequence of bloom filter A
        :param bit_seq_B: bit sequence of bloom filter B
        :return: True if two bloom filters have at least one bit in common, False otherwise
        """
        bit_seq_A = np.array(list(bit_seq_A))
        bit_seq_B = np.array(list(bit_seq_B))
        return 2 * np.sum(bit_seq_A & bit_seq_B) / (np.sum(bit_seq_A) + np.sum(bit_seq_B))

    def compare_links(self):
        df_a = pd.read_csv(self.encoded_identifiers_file_path_A, index_col="index")
        df_b = pd.read_csv(self.encoded_identifiers_file_path_B, index_col="index")
        df_links = pd.read_csv(self.candidate_links_file_path)
        candidate_links = pd.MultiIndex.from_frame(df_links, names=['index', 'index'])
        print(type(candidate_links))
        print(candidate_links)
        comparer = recordlinkage.Compare()
        # comp.exact('given_name', 'given_name', label='given_name')
        # comp.exact('surname', 'surname', label='surname')
        # comp.exact('address_1_num', 'address_1_num', label='address_1_num')
        # comp.exact('address_2', 'address_2', label='address_2')
        # comp.exact('suburb', 'suburb', label='suburb')
        # comp.exact('state_postcode', 'state_postcode', label='state_postcode')
        comparer.add(CompareBitarray('given_name', 'given_name', label='given_name'))
        comparer.add(CompareBitarray('surname', 'surname', label='surname'))
        comparer.add(CompareBitarray('address_1_num', 'address_1_num', label='address_1_num'))
        comparer.add(CompareBitarray('address_2', 'address_2', label='address_2'))
        comparer.add(CompareBitarray('suburb', 'suburb', label='suburb'))
        comparer.add(CompareBitarray('state_postcode', 'state_postcode', label='state_postcode'))
        df_compare = comparer.compute(candidate_links, df_a, df_b)
        print(df_compare)
        print(type(df_compare))
        # Save all compared links
        df_compare.to_csv(self.compared_links_file_path, compression='zip')
        return self.compared_links_file_path

    def identify_record_linkage(self):
        df_compare = pd.read_csv(self.compared_links_file_path, index_col=[0, 1])
        df_matched = df_compare[(df_compare.T > self.threshold).all()]
        df_matched.to_csv(self.matched_links_file_path)
        return self.matched_links_file_path


class CompareBitarray(BaseCompareFeature):
    def __init__(self, left_on, right_on, threshold=0.7, *args, **kwargs):
        super(CompareBitarray, self).__init__(left_on, right_on, *args, **kwargs)
        self.threshold = threshold

    def _compute_vectorized(self, s1, s2):
        # sim = (s1 == s2).astype(float)
        # return sim
        a_counts = s1.apply(lambda x: x.count('1')).values
        b_counts = s2.apply(lambda x: x.count('1')).values

        intersections = [sum(1 for a, b in zip(list(s1_val), list(s2_val)) if a == '1' and b == '1') for s1_val, s2_val
                         in zip(s1, s2)]

        dice_coefficients = [2 * intersection / (a + b) if (a + b) > 0 else 0 for intersection, a, b in
                             zip(intersections, a_counts, b_counts)]
        print(1)
        return pd.Series(dice_coefficients)

