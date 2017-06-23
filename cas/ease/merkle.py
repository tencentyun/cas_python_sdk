import hashlib
import mmap
import os
import binascii

class MerkleTree(object):

    def __init__(self, hash_list=None, hash_func=hashlib.sha256):
        self.hash_list = hash_list or []
        self.hash_func = hash_func

    def append(self, block_hash):
        self.hash_list.append(block_hash)
        return self

    def load(self, hash_list):
        self.hash_list = hash_list
        return self

    def dump(self, delimiter=' '):
        return delimiter.join(self.hash_list)

    def digest(self):
        if len(self.hash_list) < 1:
            return ''

        hashes = list(self.hash_list)
        while len(hashes) > 1:
            new_hashes = list()

            while True:
                if len(hashes) > 1:
                    md = self.hash_func()
                    md.update(hashes.pop(0))
                    md.update(hashes.pop(0))
                    #new_hashes.append(md.hexdigest())
                    new_hashes.append(md.digest())
                elif len(hashes) > 0:
                    new_hashes.append(hashes.pop(0))
                else:
                    break

            hashes = new_hashes
        return binascii.hexlify(hashes[0])


class TreeHashGenerator(object):

    MEGA_BYTE = 1024 * 1024

    def __init__(self, block_size=MEGA_BYTE, hash_func=hashlib.sha256):
        self.block_size = block_size
        self.hash_func = hash_func
        self.stream = hash_func()
        self.remain = block_size
        self.tree = MerkleTree(hash_func=hash_func)

    def update(self, data, offset=0, length=None):
        length = length or len(data)
        while length > 0:
            if length >= self.remain:
                self.stream.update(data[offset:offset + self.remain])
                #self.tree.append(self.stream.hexdigest())
                self.tree.append(self.stream.digest())
                self.stream = self.hash_func()
                offset += self.remain
                length -= self.remain
                self.remain = self.block_size
            else:
                self.stream.update(data[offset:offset + length])
                self.remain -= length
                length = 0

    def generate(self):
        if self.remain != self.block_size:
            #self.tree.append(self.stream.hexdigest())
            self.tree.append(self.stream.digest())
            self.stream = self.hash_func()

        result = self.tree

        self.remain = self.block_size
        self.tree = MerkleTree(hash_func=self.hash_func)

        return result
