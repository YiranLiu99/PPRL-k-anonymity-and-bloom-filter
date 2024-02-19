import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import recordlinkage as rl
from recordlinkage.index import Full

from run import participant


# Evaluate the results of record linkage matching.
# My approach first narrows down the candidate links by matching anonymized datasets, then encrypts and encodes identifiers into bloom filters. Subsequently, record linkage is performed by matching the bitarrays of bloom filters.
# Therefore, compare 2 methods. (Method A) matching anonymized datasets first and then comparing bloom filters, and (Method B) directly comparing bloom filters without matching anonymized datasets
# Evaluate the matching results of these two methods.
# Evaluation metrics include: Recall, Precision, and F-Score.
# Evaluation methods includes:
# Evaluation method (1): Changing the k value. k = 3, 5, 7, 9, 11, 13, 15.
# Evaluation method (2): Varying dataset sizes. Each dataset size: 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500.
# Evaluation method (3): Varying number of hash functions: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50.
# Plot PR curves and F-Score curves.
# The expected outcome of the evaluation is that the matching performance of Method A is better than Method B.


def evaluate_match_result(matched_links_path, dataset_A_path, dataset_B_path):
    df_links = pd.read_csv(matched_links_path)  # identified links
    dataset_A = pd.read_csv(dataset_A_path)
    dataset_B = pd.read_csv(dataset_B_path)

    columns_list = df_links.columns.tolist()
    columns_list[0] = 'index_a'
    columns_list[1] = 'index_b'
    df_links.columns = columns_list
    mappingA = dataset_A.set_index('index')['ID'].to_dict()
    mappingB = dataset_B.set_index('index')['ID'].to_dict()

    # Check if the IDs in links match
    df_links['match'] = df_links.apply(lambda row: mappingA[row['index_a']] == mappingB[row['index_b']], axis=1)
    # actual links
    all_possible_matches = pd.merge(dataset_A, dataset_B, on='ID', how='inner')
    all_possible_matches_count = len(all_possible_matches)  # all actual links
    # Calculate True Positives (TP), False Positives (FP), False Negatives (FN)
    # TP: Correctly identified links
    # FP: Incorrectly identified links
    # FN: Actual links but not identified
    # TN is not applicable in this context as we are not distinguishing between non-matching pairs.
    correct_links_count = df_links['match'].sum()  # TP
    incorrect_links_count = len(df_links) - correct_links_count  # FP
    not_identified_count = all_possible_matches_count - correct_links_count  # FN
    # Calculate Precision, Recall, F-Score
    precision = correct_links_count / (correct_links_count + incorrect_links_count) if correct_links_count + incorrect_links_count > 0 else 0
    recall = correct_links_count / (correct_links_count + not_identified_count) if correct_links_count + not_identified_count > 0 else 0
    f_score = 2 * precision * recall / (precision + recall) if precision + recall > 0 else 0

    print(f'Number of correctly identified links (True Positives): {correct_links_count}')
    print(f'Number of incorrectly identified links (False Positives): {incorrect_links_count}')
    print(f'Number of not identified links (False Negatives): {not_identified_count}')
    print(f'Number of all actual links: {all_possible_matches_count}')
    print(f'Precision: {precision}')
    print(f'Recall: {recall}')
    print(f'F-Score: {f_score}')

    return precision, recall, f_score, all_possible_matches_count


def plot_result_one_curve(measure_list, title):
    """
    Plot the F-Score and PR curve.
    :param measure_list: list structure: [[threshold, precision, recall, f_score, all_possible_matches_count]]
    :param threshold:
    :param title:
    :return:
    """
    measure_list = np.array(measure_list)
    # plot the f_score
    plt.plot(measure_list[:, 0], measure_list[:, 3])
    plt.xlabel('threshold')
    plt.ylabel('F-Score')
    plt.title(f'F-Score for {title}')
    plt.show()
    # plot PR curve
    plt.plot(measure_list[:, 2], measure_list[:, 1])
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title(f'PR curve for {title}')
    plt.show()


def plot_result_multiple_curves(measure_lists, curve_titles, image_title):
    """
    Plot the F-Score and PR curve for multiple measure lists.
    :param measure_lists: list of measure lists. Each measure list has the structure: [[threshold, precision, recall, f_score, all_possible_matches_count]]
    :param curve_titles: list of titles corresponding to each measure list
    :param image_title: title of the plot
    :return:
    """
    # plot the f_score
    cnt1=0
    for measure_list, curve_title in zip(measure_lists, curve_titles):
        if cnt1<4:
            linstyle='-'
        else:
            linstyle='--'
        measure_list = np.array(measure_list)
        plt.plot(measure_list[:, 0], measure_list[:, 3], label=curve_title, linestyle=linstyle)
        cnt1+=1
    plt.xlabel('threshold')
    plt.ylabel('F-Score')
    plt.title(f'F-Score for {image_title}')
    plt.legend()
    plt.show()
    # plot PR curve
    cnt2=0
    for measure_list, curve_title in zip(measure_lists, curve_titles):
        if cnt2<4:
            linstyle='-'
        else:
            linstyle='--'
        measure_list = np.array(measure_list)
        plt.plot(measure_list[:, 2], measure_list[:, 1], label=curve_title, linestyle=linstyle)
        cnt2+=1
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title(f'PR curve for {image_title}')
    plt.legend()
    plt.show()



if __name__ == '__main__':
    print("Evaluation of record linkage matching results")
    print("Changing k, number of hash functions, and dataset size under different threshold")

    # # evaluate match result without anonymization
    # encoded_identifiers_file_path_A = 'test_dataset/encoded_identifiers_A.zip'
    # encoded_identifiers_file_path_B = 'test_dataset/encoded_identifiers_B.zip'
    # candidate_links_path = 'test_dataset/all_links.zip'
    # matched_links_file_path = 'test_dataset/matched_links.csv'
    # identify_record_linkage(encoded_identifiers_file_path_A, encoded_identifiers_file_path_B, matched_links_file_path, candidate_links_path, threshold=0.8)
    # matched_links_path = 'test_dataset/matched_links.csv'
    # dataset_A_path = 'test_dataset/change_k/dataset_A.csv'
    # dataset_B_path = 'test_dataset/change_k/dataset_B.csv'
    # precision, recall, f_score, all_possible_matches_count = evaluate_match_result(matched_links_path, dataset_A_path, dataset_B_path)


    # matched_links_path = 'test_dataset/change_k/no_anonymization/matched_links.csv'
    # dataset_A_path = 'test_dataset/change_k/dataset_A.csv'
    # dataset_B_path = 'test_dataset/change_k/dataset_B.csv'
    # precision, recall, f_score, all_possible_matches_count = evaluate_match_result(matched_links_path, dataset_A_path, dataset_B_path)


    # # evaluate match result changing k(k=5, 10, 15, 20)
    # # measure the performance of record linkage for different k values under different threshold, save the results to a list.
    # # list structure: [[threshold, precision, recall, f_score, all_possible_matches_count]]
    measure_result_lists = []
    curve_title_list = []
    for k in [5, 10, 15, 20]:
        print(f'k={k}')
        measure_result_list = []
        for threshold in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]:
            print(f'threshold={threshold}')
            matched_links_path = f'test_dataset/change_k/k_{k}/matched_links_differ_threshold/matched_links_threshold_{int(threshold * 10)}.csv'
            dataset_A_path = f'test_dataset/change_k/dataset_A.csv'
            dataset_B_path = f'test_dataset/change_k/dataset_B.csv'
            precision, recall, f_score, all_possible_matches_count = evaluate_match_result(matched_links_path,
                                                                                           dataset_A_path,
                                                                                           dataset_B_path)
            measure_result_list.append([threshold, precision, recall, f_score, all_possible_matches_count])
        measure_result_lists.append(measure_result_list)
        curve_title_list.append(f"k={k} with anonymization")
    for k in [0]:
        print(f'k={k}')
        measure_result_list = []
        for threshold in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]:
            print(f'threshold={threshold}')
            matched_links_path = f'test_dataset/change_k/no_anonymization/matched_links_differ_threshold/matched_links_threshold_{int(threshold * 10)}.csv'
            dataset_A_path = f'test_dataset/change_k/dataset_A.csv'
            dataset_B_path = f'test_dataset/change_k/dataset_B.csv'
            precision, recall, f_score, all_possible_matches_count = evaluate_match_result(matched_links_path,
                                                                                           dataset_A_path,
                                                                                           dataset_B_path)
            measure_result_list.append([threshold, precision, recall, f_score, all_possible_matches_count])
        measure_result_lists.append(measure_result_list)
        curve_title_list.append(f"k={k} without anonymization")
    plot_result_multiple_curves(measure_result_lists, curve_title_list, "different k-Anonymity")

    # measure_result_lists = []
    # curve_title_list = []
    # for datasize in [1500, 2500, 3500, 4500]:
    #     print(f'datasize={datasize}')
    #     measure_result_list = []
    #     for threshold in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]:
    #         print(f'threshold={threshold}')
    #         matched_links_path = f'test_dataset/change_data_size/data_size_{datasize}/with_anonymization/matched_links_differ_threshold/matched_links_threshold_{int(threshold * 10)}.csv'
    #         dataset_A_path = f'test_dataset/change_data_size/data_size_{datasize}/dataset_A.csv'
    #         dataset_B_path = f'test_dataset/change_data_size/data_size_{datasize}/dataset_B.csv'
    #         precision, recall, f_score, all_possible_matches_count = evaluate_match_result(matched_links_path,
    #                                                                                        dataset_A_path,
    #                                                                                        dataset_B_path)
    #         measure_result_list.append([threshold, precision, recall, f_score, all_possible_matches_count])
    #     measure_result_lists.append(measure_result_list)
    #     curve_title_list.append(f"datasize={datasize} with anonymization")
    # for datasize in [1500, 2500, 3500, 4500]:
    #     print(f'datasize={datasize}')
    #     measure_result_list = []
    #     for threshold in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]:
    #         print(f'threshold={threshold}')
    #         matched_links_path = f'test_dataset/change_data_size/data_size_{datasize}/no_anonymization/matched_links_differ_threshold/matched_links_threshold_{int(threshold * 10)}.csv'
    #         dataset_A_path = f'test_dataset/change_data_size/data_size_{datasize}/dataset_A.csv'
    #         dataset_B_path = f'test_dataset/change_data_size/data_size_{datasize}/dataset_B.csv'
    #         precision, recall, f_score, all_possible_matches_count = evaluate_match_result(matched_links_path,
    #                                                                                        dataset_A_path,
    #                                                                                        dataset_B_path)
    #         measure_result_list.append([threshold, precision, recall, f_score, all_possible_matches_count])
    #     measure_result_lists.append(measure_result_list)
    #     curve_title_list.append(f"datasize={datasize} without anonymization")
    # plot_result_multiple_curves(measure_result_lists, curve_title_list, "different data size")

