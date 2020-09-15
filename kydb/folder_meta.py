from typing import List


class FolderMetaMixin:
    """ Used for providing list dir on a folder
        for DB implementations that does not
        have such mechanics
    """

    def _mkdir(self, folders: List[str]):
        curr_folder = '/'
        for folder in folders:
            meta_path = self._folder_meta_path(curr_folder, folder)
            if not self.exists(meta_path):
                self.set_raw(meta_path, self._serialise(True))

            curr_folder += folder + '/'

    @staticmethod
    def _folder_meta_path(folder: str, subfolder: str):
        return folder + '.folder-' + subfolder

    @staticmethod
    def _is_folder_meta(objname: str):
        return objname.startswith('.folder-')

    def list_dir(self, folder: str, include_dir=True):
        for objname in self.list_dir_raw(folder):
            if self._is_folder_meta(objname):
                if include_dir:
                    # Cut out the folder meta prefix
                    # and add /
                    yield objname[8:] + '/'
            else:
                yield objname

    def __setitem__(self, key: str, value):
        key = self._ensure_slashes(key)[:-1]
        parts = key.split('/')[1:]
        if parts:
            parts = parts[:-1]

        self._mkdir(parts)
        super().__setitem__(key, value)
