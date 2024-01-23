import participant

if __name__ == '__main__':

    # prepare dataset
    date_file_path_A = '../dataset/dataset_A/dataset_A.csv'
    date_file_path_B = '../dataset/dataset_B/dataset_B.csv'
    anonymized_file_dir_path_A = '../dataset/dataset_A/'
    anonymized_file_dir_path_B = '../dataset/dataset_B/'
    hierarchy_file_dir_path = '../dataset/hierarchy/'
    quasi_identifiers = ['sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass', 'occupation']
    sensitive_attributes = ['salary-class']
    identifier = ['given_name', 'surname', 'street_number', 'address_1', 'address_2', 'suburb', 'postcode', 'state', 'soc_sec_id']

    # initialize two data holders
    data_holder_A = participant.DataHolder('A',
                                           date_file_path_A, anonymized_file_dir_path_A, hierarchy_file_dir_path,
                                           quasi_identifiers, sensitive_attributes, identifier, k=5)
    data_holder_B = participant.DataHolder('B',
                                           date_file_path_B, anonymized_file_dir_path_B, hierarchy_file_dir_path,
                                           quasi_identifiers, sensitive_attributes, identifier, k=5)

    # Two data holders anonymize their data. Remove sensitive attributes and identifiers.
    # Then send anonymized data to classifier1
    anonymized_data_no_sa_ident_path_A = data_holder_A.send_anonymized_data()
    anonymized_data_no_sa_ident_path_B = data_holder_B.send_anonymized_data()

    print(anonymized_data_no_sa_ident_path_A)
    print(anonymized_data_no_sa_ident_path_B)

    # Classifier1 receives anonymized data from two data holders, and find candidate links
    # Then send candidate records back to data holders
    classifier1 = participant.Classifier1(anonymized_data_no_sa_ident_path_A, anonymized_data_no_sa_ident_path_B, hierarchy_file_dir_path, quasi_identifiers)
    candidate_links, candidate_record_set_A, candidate_record_set_B = classifier1.send_candidate_links()

    data_holder_A.receive_candidate_records(candidate_record_set_A)
    data_holder_B.receive_candidate_records(candidate_record_set_B)