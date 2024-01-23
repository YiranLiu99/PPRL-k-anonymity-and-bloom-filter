import csv

import pandas as pd

from k_anonymize import mondrian
from record_linkage.block_links import block_data


class DataHolder:
    def __init__(self, holder_name, original_data_path, anonymized_data_dir_path, hierarchy_file_dir_path,
                 quasi_identifiers, sensitive_attributes, identifier, k=5):
        self.holder_name = holder_name
        self.original_data_path = original_data_path
        self.anonymized_data_dir_path = anonymized_data_dir_path
        self.anonymized_data_path = f'{self.anonymized_data_dir_path}k_{k}_anonymized_dataset_{self.holder_name}.csv'
        self.anonymized_data_no_sa_ident_path = f'{self.anonymized_data_dir_path}k_{k}_anonymized_dataset_{self.holder_name}_no_sa_ident.csv'
        self.candidate_records_index_file_path = f'{self.anonymized_data_dir_path}candidate_records_index_{self.holder_name}.csv'
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
        df.to_csv(self.anonymized_data_no_sa_ident_path, index=False)
        print(f'Remove sensitive attributes and identifiers for dataholder {self.holder_name} successfully! '
              f'Saved at {self.anonymized_data_no_sa_ident_path}')

    def send_anonymized_data(self):
        self.anonymize_data_and_save()
        self.remove_sensitive_attributes_and_identifiers()
        return self.anonymized_data_no_sa_ident_path

    def receive_candidate_records(self, candidate_record_set):
        with open(self.candidate_records_index_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            for element in candidate_record_set:
                csv_writer.writerow([element])
        print(f'Received candidate records from classifier1 for dataholder {self.holder_name} successfully! '
              f'Saved at {self.candidate_records_index_file_path}')

class Classifier1:
    def __init__(self, anonymized_data_path_A, anonymized_data_path_B, hierarchy_file_dir_path, qs_list):
        self.anonymized_data_path_A = anonymized_data_path_A
        self.anonymized_data_path_B = anonymized_data_path_B
        self.hierarchy_file_dir_path = hierarchy_file_dir_path
        self.qs_list = qs_list

    def send_candidate_links(self):
        (candidate_links,
         candidate_record_set_A,
         candidate_record_set_B) = block_data(self.anonymized_data_path_A,
                                              self.anonymized_data_path_B,
                                              self.hierarchy_file_dir_path,
                                              self.qs_list)
        return candidate_links, candidate_record_set_A, candidate_record_set_B
