import participant

if __name__ == '__main__':

    k = 5
    threshold = 0.8

    # prepare dataset
    date_file_path_A = '../dataset/dataset_A/dataset_A.csv'
    date_file_path_B = '../dataset/dataset_B/dataset_B.csv'
    classifier_data_dir_path = '../dataset/classifier_data/'
    anonymized_file_dir_path_A = '../dataset/dataset_A/'
    anonymized_file_dir_path_B = '../dataset/dataset_B/'
    hierarchy_file_dir_path = '../dataset/hierarchy/'
    anonymized_data_no_sa_ident_path_A = f'../dataset/dataset_A/k_{k}_anonymized_dataset_A_no_sa_ident.csv'
    anonymized_data_no_sa_ident_path_B = f'../dataset/dataset_B/k_{k}_anonymized_dataset_B_no_sa_ident.csv'
    encoded_identifiers_file_path_A = '../dataset/dataset_A/encoded_identifiers_A.zip'
    encoded_identifiers_file_path_B = '../dataset/dataset_B/encoded_identifiers_B.zip'
    candidate_links_path = '../dataset/classifier_data/candidate_links.zip'
    compared_links_file_path = '../dataset/compared_links.zip'
    matched_links_file_path = '../dataset/matched_links.csv'

    quasi_identifiers = ['sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass', 'occupation']
    sensitive_attributes = ['salary-class']
    identifier = ['given_name', 'surname', 'street_number', 'address_1', 'address_2', 'suburb', 'postcode', 'state', 'soc_sec_id']

    # initialize two data holders
    data_holder_A = participant.DataHolder('A',
                                           date_file_path_A, anonymized_file_dir_path_A, hierarchy_file_dir_path,
                                           quasi_identifiers, sensitive_attributes, identifier, k=k)
    data_holder_B = participant.DataHolder('B',
                                           date_file_path_B, anonymized_file_dir_path_B, hierarchy_file_dir_path,
                                           quasi_identifiers, sensitive_attributes, identifier, k=k)

    # Two data holders anonymize their data. Remove sensitive attributes and identifiers.
    # Then send anonymized data to classifier1
    anonymized_data_no_sa_ident_path_A = data_holder_A.send_anonymized_data()
    anonymized_data_no_sa_ident_path_B = data_holder_B.send_anonymized_data()
    print("=====================================")

    # Classifier1 receives anonymized data from two data holders, and find candidate links
    # Then send candidate records back to data holders
    classifier1 = participant.Classifier1(anonymized_data_no_sa_ident_path_A, anonymized_data_no_sa_ident_path_B, classifier_data_dir_path, hierarchy_file_dir_path, quasi_identifiers)
    candidate_links_path, candidate_record_set_path_A, candidate_record_set_path_B = classifier1.send_candidate_links()
    print("=====================================")

    # Data holders encode identifiers into bloom filters
    encoded_identifiers_file_path_A = data_holder_A.send_encode_identifiers_in_bloom_filter()
    encoded_identifiers_file_path_B = data_holder_B.send_encode_identifiers_in_bloom_filter()
    print("=====================================")

    # Data holders send encoded identifiers to classifier2. Classifier1 send candidate links index to classifier2
    classifier2 = participant.Classifier2(encoded_identifiers_file_path_A, encoded_identifiers_file_path_B,
                                          candidate_links_path, compared_links_file_path, matched_links_file_path, threshold=threshold)
    compared_links_file_path = classifier2.compare_links()
    matched_links_file_path = classifier2.identify_record_linkage()