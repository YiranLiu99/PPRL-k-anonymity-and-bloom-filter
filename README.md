# record-linkage-bloom

Hybrid Approach for Privacy-Preserving Record Linkage: Integrating k-Anonymity and Bloom Filters

1. Remove sensitive information, such as social ID.
2. Employ k-anonymization, dividing the data into numerous groups.
3. Implement blocking to significantly reduce candidate links.
4. Utilize a Bloom filter to compare the remaining candidate links. For this dataset, compare surname + given name.
5. The remaining candidate links constitute the matched results.