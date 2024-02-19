import hashlib
import hmac
import numpy as np


def split_ngrams(value, n=2):
    """
    Split string into ngrams
    :param value: string to be split
    :param n: ngram size
    :return: list of ngrams
    """
    padded_str = ' ' + value + ' '
    ngrams = [padded_str[i:i + 2] for i in range(len(padded_str) - 1)]
    return ngrams


def encode_bloom(value, size=200, num_hash=5):
    """
    Encode string in bloom filter
    :return:
    """
    ngrams = split_ngrams(value)
    bf = BloomFilter(size=size, num_hash=num_hash)
    for ngram in ngrams:
        bf.add(ngram)
    bit_string = ''.join(map(lambda x: '1' if x else '0', bf.get_bit_seq()))
    return bit_string


def md5_hashing(value, secret_key):
    """
    encrypt value with md5
    :param value: value to be encrypted
    :param secret_key: secret key
    :return: encrypted value in integer
    """
    secret_key_bytes = bytes(secret_key, 'utf-8')
    value_bytes = bytes(value, 'utf-8')
    hmac_md5 = hmac.new(secret_key_bytes, value_bytes, hashlib.md5)
    return int(hmac_md5.hexdigest(), 16)

def sha1_hashing(value, secret_key):
    """
    encrypt value with sha1
    :param value: value to be encrypted
    :param secret_key: secret key
    :return: encrypted value in integer
    """
    secret_key_bytes = bytes(secret_key, 'utf-8')
    value_bytes = bytes(value, 'utf-8')
    hmac_sha1 = hmac.new(secret_key_bytes, value_bytes, hashlib.sha1)
    return int(hmac_sha1.hexdigest(), 16)

class BloomFilter:
    """
    Bloom Filter using double hashing
    """

    def __init__(self, size=1000, num_hash=3):
        self.size = size  # length of bit sequence
        self.num_hash = num_hash  # number of hash functions. gi(x) = (h1(x) + i*h2(x)) % size (i=0,1,2,...,num_hash-1)
        self.bit_seq = np.zeros(self.size, dtype=np.bool_)
        self.num_element = 0

    def add(self, value, secret_key="secret_key"):
        """
        add value to the bloom filter using double hashing
        hash_function1 is sha1, hash_function2 is md5

        :param value:
        :param secret_key:
        :return:
        """
        pos_hash_values = self._double_hashing(value, secret_key)
        for pos in pos_hash_values:
            self.bit_seq[pos] = True
        self.num_element += 1

    def look_up(self, value, secret_key="secret_key"):
        pos_hash_values = self._double_hashing(value, secret_key)
        for pos in pos_hash_values:
            if not self.bit_seq[pos]:
                return False
        return True

    def error_rate(self):
        return (1 - np.exp(-self.num_hash * self.num_element / self.size)) ** self.num_hash

    def get_bit_seq(self):
        return self.bit_seq

    def get_num_element(self):
        return self.num_element

    def get_bit_seq_length(self):
        return self.size

    def get_num_bit_one(self):
        return np.sum(self.bit_seq)

    def exist_collision(self):
        """
        Check if there is any collision in the bit sequence
        :return: True if there is collision, False otherwise
        """
        num_bit_one_no_collision = self.num_element * self.num_hash
        num_bit_one = self.get_num_bit_one()
        if num_bit_one_no_collision == num_bit_one:
            return False
        else:
            return True

    def _double_hashing(self, value, secret_key="secret_key"):
        if value is None:
            value = 0
        h1_value = sha1_hashing(value, secret_key)
        h2_value = md5_hashing(value, secret_key)
        pos_hash_values = []
        for i in range(0, self.num_hash):
            pos_hash_values.append((h1_value + i * h2_value) % self.size)
        return pos_hash_values


if __name__ == "__main__":
    print("Testing Bloom Filter")

    # # generate random strings
    # import random
    # import string
    # import time
    #
    # random.seed(0)
    # num_strings = 100
    # length_string = 20
    # strings = []
    # for i in range(num_strings):
    #     characters = string.ascii_letters + string.digits
    #     unique_string = ''.join(random.sample(characters, length_string))
    #     strings.append(unique_string)
    #
    # cnt = 0
    # for string in strings:
    #     a = encode_bloom(string, size=1000, num_hash=10)
    #     if a == -1:
    #         cnt += 1
    #     print(string)
    #     print('======================')
    # print(f'{cnt}/{num_strings} strings have collision')
