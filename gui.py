import streamlit as st
import pandas as pd
from run import participant


def init_session_state():
    if 'anonymized_df_A' not in st.session_state:
        st.session_state['anonymized_df_A'] = None
    if 'anonymized_df_B' not in st.session_state:
        st.session_state['anonymized_df_B'] = None
    if 'encoded_df_A' not in st.session_state:
        st.session_state['encoded_df_A'] = None
    if 'encoded_df_B' not in st.session_state:
        st.session_state['encoded_df_B'] = None
    if 'candidate_links_df' not in st.session_state:
        st.session_state['candidate_links_df'] = None
    if 'identified_links_df' not in st.session_state:
        st.session_state['identified_links_df'] = None


if __name__ == '__main__':
    print("GUI started")

    st.set_page_config(page_title='Privacy-Preserving Record Linkage (PPRL)')
    st.title('Hybrid Approach for Privacy-Preserving Record Linkage: Integrating k-Anonymity and Bloom Filters')
    # Add usage instructions right below the title
    st.markdown("""
            ## Usage Instructions
            1. Original dataset A and original dataset B are prepared, located at `dataset/dataset_A/dataset_A.csv` and `dataset/dataset_B/dataset_B.csv` respectively.
            2. Upload original datasets for each dataholder.
            3. Dataholders anonymize their original dataset.
            4. Classifier 1 matches candidate links between two anonymized datasets.
            5. Dataholders encrypt and encode the identifiers of their original dataset.
            6. Classifier 2 identifies record linkages using two encoded datasets and candidate links.
            7. Download matched links as a CSV file.
        """)

    init_session_state()

    # Sidebar for k-Anonymity selection
    k = st.sidebar.selectbox('Select k for k-Anonymity(default k=5)', options=['Select', 3, 5, 10, 15, 20])
    # change k to int. If k is not 'Select', set k to 5 as default
    k = int(k) if k != 'Select' else 5
    # Dice Coefficient Threshold selection
    threshold = st.sidebar.selectbox('Select Dice Coefficient threshold(default threshold=0.8)',
                                     options=['Select', 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1])
    # change dice_coefficient to float. If dice_coefficient is not 'Select', set dice_coefficient to 0.8 as default
    threshold = float(threshold) if threshold != 'Select' else 0.8

    date_file_path_A = 'dataset/dataset_A/dataset_A.csv'
    date_file_path_B = 'dataset/dataset_B/dataset_B.csv'
    classifier_data_dir_path = 'dataset/classifier_data/'
    anonymized_file_dir_path_A = 'dataset/dataset_A/'
    anonymized_file_dir_path_B = 'dataset/dataset_B/'
    hierarchy_file_dir_path = 'dataset/hierarchy/'
    anonymized_data_no_sa_ident_path_A = f'dataset/dataset_A/k_{k}_anonymized_dataset_A_no_sa_ident.csv'
    anonymized_data_no_sa_ident_path_B = f'dataset/dataset_B/k_{k}_anonymized_dataset_B_no_sa_ident.csv'
    encoded_identifiers_file_path_A = 'dataset/dataset_A/encoded_identifiers_A.zip'
    encoded_identifiers_file_path_B = 'dataset/dataset_B/encoded_identifiers_B.zip'
    candidate_links_path = 'dataset/classifier_data/candidate_links.zip'
    compared_links_file_path = 'dataset/compared_links.zip'
    matched_links_file_path = 'dataset/matched_links.csv'
    quasi_identifiers = ['sex', 'age', 'race', 'marital-status', 'education', 'native-country', 'workclass',
                         'occupation']
    sensitive_attributes = ['salary-class']
    identifier = ['given_name', 'surname', 'street_number', 'address_1', 'address_2', 'suburb', 'postcode', 'state',
                  'soc_sec_id']

    # Split the page into two columns for buttons and displaying data
    left_column, right_column = st.columns(2)

    # Use the left column for input and buttons
    with left_column:
        # Dataholder A input
        st.subheader('Dataholder A')
        org_dataset_A = st.file_uploader('Upload original dataset for dataholder A', type=['csv'], key='org_dataset_A')
        if org_dataset_A is not None:
            data_holder_A = participant.DataHolder('A', date_file_path_A, anonymized_file_dir_path_A,
                                                   hierarchy_file_dir_path,
                                                   quasi_identifiers, sensitive_attributes, identifier, k=k)
            st.write('Original dataset_A uploaded successfully')
        # Anonymize data button
        if st.button('Anonymize data', key='anonymized_dataset_A'):
            anonymized_data_no_sa_ident_path_A = data_holder_A.send_anonymized_data()
            st.session_state['anonymized_df_A'] = pd.read_csv(anonymized_data_no_sa_ident_path_A)
            st.write('Original dataset_A anonymized successfully')
        # Encrypt and encode identifiers button
        if st.button('Encrypt and encode identifiers', key='encrypt_dataset_A'):
            encoded_identifiers_file_path_A = data_holder_A.send_encode_identifiers_in_bloom_filter()
            st.session_state['encoded_df_A'] = pd.read_csv(encoded_identifiers_file_path_A)
            st.write('Identifiers of dataset_A encrypted and encoded successfully')

        # Dataholder B input
        st.subheader('Dataholder B')
        org_dataset_B = st.file_uploader('Upload original dataset for dataholder B', type=['csv'], key='org_dataset_B')
        if org_dataset_B is not None:
            data_holder_B = participant.DataHolder('B', date_file_path_B, anonymized_file_dir_path_B,
                                                   hierarchy_file_dir_path,
                                                   quasi_identifiers, sensitive_attributes, identifier, k=k)
            st.write('Original dataset_B uploaded successfully')
        # Anonymize data button
        if st.button('Anonymize data', key='anonymized_dataset_B'):
            anonymized_data_no_sa_ident_path_B = data_holder_B.send_anonymized_data()
            st.session_state['anonymized_df_B'] = pd.read_csv(anonymized_data_no_sa_ident_path_B)
            st.write('Original dataset_B anonymized successfully')
        # Encrypt and encode identifiers button
        if st.button('Encrypt and encode identifiers', key='encrypt_dataset_B'):
            encoded_identifiers_file_path_B = data_holder_B.send_encode_identifiers_in_bloom_filter()
            st.session_state['encoded_df_B'] = pd.read_csv(encoded_identifiers_file_path_B)
            st.write('Identifiers of dataset_B encrypted and encoded successfully')

        # Classifier 1
        st.subheader('Classifier 1')
        if st.button('Match anonymized data'):
            classifier1 = participant.Classifier1(anonymized_data_no_sa_ident_path_A,
                                                  anonymized_data_no_sa_ident_path_B, classifier_data_dir_path,
                                                  hierarchy_file_dir_path, quasi_identifiers)
            candidate_links_path, candidate_record_set_path_A, candidate_record_set_path_B = classifier1.send_candidate_links()
            st.session_state['candidate_links_df'] = pd.read_csv(candidate_links_path)
            st.write('2 anonymized datasets matched by Classifier 1')

        # Classifier 2
        st.subheader('Classifier 2')
        if st.button('Identify record linkages'):
            classifier2 = participant.Classifier2(encoded_identifiers_file_path_A, encoded_identifiers_file_path_B,
                                                  candidate_links_path, compared_links_file_path,
                                                  matched_links_file_path, threshold=threshold)
            classifier2.compare_links()
            matched_links_file_path = classifier2.identify_record_linkage()
            st.session_state['identified_links_df'] = pd.read_csv(matched_links_file_path)
            st.write('Record linkages identified by Classifier 2')

    # Use the right column to display CSV data
    with right_column:
        st.subheader('Data Preview')
        dataframe_height = 200
        # Display original CSV file content if uploaded
        if org_dataset_A is not None:
            st.write('Original dataset_A Preview:')
            df_a = pd.read_csv(org_dataset_A)
            st.dataframe(df_a, height=dataframe_height)
        if org_dataset_B is not None:
            st.write('Original dataset_B Preview:')
            df_b = pd.read_csv(org_dataset_B)
            st.dataframe(df_b, height=dataframe_height)
        # Display the anonymized data if available
        if st.session_state['anonymized_df_A'] is not None:
            st.write('Anonymized dataset_A Preview:')
            df_anonymized_A = pd.read_csv(anonymized_data_no_sa_ident_path_A)
            st.dataframe(df_anonymized_A, height=dataframe_height)
        if st.session_state['anonymized_df_B'] is not None:
            st.write('Anonymized dataset_B Preview:')
            df_anonymized_B = pd.read_csv(anonymized_data_no_sa_ident_path_B)
            st.dataframe(df_anonymized_B, height=dataframe_height)
        # Display the candidate links if available
        if st.session_state['candidate_links_df'] is not None:
            st.write('Candidate Links Preview:')
            df_candidate_links = pd.read_csv(candidate_links_path)
            st.dataframe(df_candidate_links, height=dataframe_height)
        # Display the encrypted and encoded data if available
        if st.session_state['encoded_df_A'] is not None:
            st.write('Encrypted and Encoded dataset_A Preview:')
            df_encoded_A = pd.read_csv(encoded_identifiers_file_path_A)
            st.dataframe(df_encoded_A, height=dataframe_height)
        if st.session_state['encoded_df_B'] is not None:
            st.write('Encrypted and Encoded dataset_B Preview:')
            df_encoded_B = pd.read_csv(encoded_identifiers_file_path_B)
            st.dataframe(df_encoded_B, height=dataframe_height)
        # Display the matched links if available
        if st.session_state['identified_links_df'] is not None:
            st.write('Identified Links Preview:')
            df_identified_links = pd.read_csv(matched_links_file_path)
            st.dataframe(df_identified_links, height=dataframe_height)
        # Download record linkage button
        if st.session_state['identified_links_df'] is not None:
            st.download_button(
                label="Download Record Linkage",
                data=st.session_state['identified_links_df'].to_csv().encode('utf-8'),
                file_name='matched_links.csv',
                mime='text/csv',
                key='download_matched_links'
            )
