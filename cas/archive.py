# -*- coding: utf-8 -*-


class Archive:

    def __init__(self, vault, archive_id):
        self.vault = vault
        self.archive_id = archive_id

    def delete(self):
        return self.vault.delete_archive(self.archive_id)

    def initiate_archive_retrieval(self, desc=None, byte_range=None, tier=None):
        return self.vault.retrieve_archive(self.archive_id, desc=desc, byte_range=byte_range, tier=tier)

    def get_vault(self):
        return self.vault
