# Hybrid Approach for Privacy-Preserving Record Linkage: Integrating k-Anonymity and Bloom Filters

## Usage
`conda create --name rl-py10 python=3.10` <br>
`conda activate rl-py10` <br>
`pip install -r requirements.txt` <br>
`streamlit run .\gui.py` <br>

Follow the instructions in the GUI interface. Original dataset A and original dataset B are prepared, located at `dataset/dataset_A/dataset_A.csv` and `dataset/dataset_B/dataset_B.csv` respectively.


## Abstract
We introduce an approach for Privacy-Preserving Record Linkage (PPRL) by integrating k-anonymity and bloom filters to balance privacy protection and matching accuracy. The approach begins by applying k-anonymity to quasi-identifiers of the datasets to narrow the scope of candidate links by matching similar records between two anonymized datasets. The second step encrypts the identifiers and encodes them into Bloom filters for efficient and secure matching. We designed a multi-party protocol that assigns several key steps to different participants, ensuring data privacy. This approach aims to balance data utility and privacy, providing an effective solution to the challenges of PPRL.


### Note on Evaluation Dataset
Please note that due to the large size of the evaluation datasets, it has not been included in this repository. If you require access to the full evaluation datasets, please contact https://github.com/YiranLiu99. Thank you for your understanding.