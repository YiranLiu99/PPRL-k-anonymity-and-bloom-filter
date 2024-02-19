import glob
import os
import pandas as pd


class HierarchyTreeNode:
    def __init__(self, value, parent=None, is_leaf=False, leaf_id='0', level=0):
        self.value = value
        self.level = level
        self.is_leaf = is_leaf
        # leaf_num is 0 if it is not a leaf
        self.leaf_id = leaf_id
        self.parent = parent
        self.children = []
        self.covered_subtree_nodes = set()

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return str(self.value)


class HierarchyTree:
    def __init__(self, file_path):
        df = pd.read_csv(file_path, header=None)
        self.hierarchy_type = file_path.split('_')[2]
        self.node_dict = build_tree(df)  # keys: values in data(since each value is unique in data), values: HierarchyTreeNode
        self.root = self.node_dict['*']
        self.leaf_id_dict = self.build_leaf_id_dict()  # keys are leaf_id, values are HierarchyTreeNode(leaves only)
        self.save_covered_subtree_nodes()

    def find_node(self, value):
        """
        find the node which value is value
        :param value:
        :return:
        """
        return self.node_dict[value]

    def build_leaf_id_dict(self):
        leaf_id_dict = {}
        for node in self.node_dict.values():
            if node.is_leaf:
                leaf_id_dict[node.leaf_id] = node
        return leaf_id_dict

    def save_covered_subtree_nodes(self):
        """
        save the covered subtree nodes for each node
        :return:
        """
        for node in self.node_dict.values():
            parent = node.parent
            while parent:
                parent.covered_subtree_nodes.add(node)
                parent = parent.parent

    def check_node_covered(self, node_value, check_node_value):
        """
        check if check_node is covered by node
        :param node_value:
        :param check_node_value:
        :return:
        """
        node = self.node_dict[node_value]
        check_node = self.node_dict[check_node_value]
        if node in check_node.covered_subtree_nodes:
            return True
        else:
            return False

    def find_common_ancestor(self, leaf1_id, leaf2_id):
        """
        find the common ancestor of leaf1 and leaf2, which can help to find the generalization level
        e.g. if leaf1_id=1, leaf2_id=2 represent 'Doctorate' and 'Masters', then the common ancestor is 'Graduate'
        Then we can replace [1-2] with 'Graduate' after anonymization
        if leaf1_id=1, leaf2_id=6 represent 'Doctorate' and 'Bachelors', then the common ancestor is 'Higher education'
        Then we can replace [1-6] with 'Higher education' after anonymization
        :param leaf1_id:
        :param leaf2_id:
        :return:
        """
        leaf1 = self.leaf_id_dict[leaf1_id]
        leaf2 = self.leaf_id_dict[leaf2_id]
        ancestors = set()
        # find all ancestors of leaf1
        while leaf1:
            ancestors.add(leaf1)
            leaf1 = leaf1.parent
        # find the first ancestor of leaf2 that is also an ancestor of leaf1
        while leaf2 not in ancestors:
            leaf2 = leaf2.parent
        return leaf2


def build_tree(df):
    df.columns = [f"col_{i}" for i in range(len(df.columns))]
    # keys are values in file(since each value is unique in file), value is HierarchyTreeNode
    node_dict = {}

    # create root node, add to node_dict
    node_dict['*'] = HierarchyTreeNode(value='*', is_leaf=False, level=0, parent=None)
    for _, row in df.iterrows():
        row_list = row.tolist()
        # go from last column to first column.
        # Last column is root node. Second column are leaf nodes. First column are IDs for leaf nodes.
        row_list.reverse()
        for i, value in enumerate(row_list):
            value = str(value)
            is_leaf = False
            # Second column are leaf nodes.
            if i == len(row_list) - 2:
                is_leaf = True
            # First column are IDs for leaf nodes.
            if i != len(row_list) - 1:
                # try and except is more efficient than 'in'
                try:
                    node_dict[value]
                except KeyError:
                    new_node = HierarchyTreeNode(value=value, is_leaf=is_leaf, level=i + 1,
                                                 parent=node_dict[str(row_list[i - 1])])
                    node_dict[value] = new_node
                    node_dict[str(row_list[i - 1])].children.append(new_node)
            else:  # this is the first column representing IDs for leaf nodes
                id = value
                node_dict[str(row_list[i - 1])].leaf_id = id
    return node_dict


def build_all_hierarchy_tree(hierarchy_file_dir_path):
    hierarchy_tree_dict = {}
    files = glob.glob(os.path.join(hierarchy_file_dir_path, '*.csv'))
    for file_path in files:
        file_name = os.path.basename(file_path)
        hierarchy_type = file_name.split('_')[2].split('.')[0]
        hierarchy_tree = HierarchyTree(file_path)
        hierarchy_tree_dict[hierarchy_type] = hierarchy_tree
    return hierarchy_tree_dict
