from typing import List


class FolderMetaMixin:
    """ Used for providing list dir on a folder
        for DB implementations that does not
        have such mechanics
    """

    def _mkdir(self, folders: List[str]):
        curr_folder = '/'
        for folder in folders:
            meta_path = self._folder_meta_path(curr_folder)
            try:
                folder_meta = self[meta_path]
            except KeyError:
                folder_meta = set()

            folder_meta.add(folder)

            self.set_raw(meta_path, self._serialise(folder_meta))
            curr_folder += folder + '/'

    @staticmethod
    def _folder_meta_path(folder: str):
        return folder + '.folder-meta'

    def list_subdir(self, folder: str):
        meta_path = self._folder_meta_path(self._ensure_slashes(folder))
        try:
            for subfolder in sorted(self[meta_path]):
                yield subfolder
        except KeyError:
            pass

    def __setitem__(self, key: str, value):
        key = self._ensure_slashes(key)[:-1]
        parts = key.split('/')[1:]
        if parts:
            parts = parts[:-1]

        self._mkdir(parts)
        super().__setitem__(key, value)
