class FolderMetaMixin:
    """ Used for providing list dir on a folder
        for DB implementations that does not
        have such mechanics
    """

    def mkdir_raw(self, folder: str):
        folders = self._ensure_slashes(folder)[1:-1].split('/')
        curr_folder = '/'
        for folder in folders:
            meta_path = self._folder_meta_path(curr_folder, folder)
            if not self.exists(meta_path):
                self.set_raw(meta_path, self._serialise(True))

            curr_folder += folder + '/'

    @classmethod
    def _folder_meta_path(cls, folder: str, subfolder: str = ''):
        folder = cls._ensure_slashes(folder)

        if not subfolder:
            folder = folder[:-1]
            if '/' not in folder:
                raise KeyError('Bad folder: ' + folder)

            folder, subfolder = folder.rsplit('/', 1)
            folder += '/'

        return folder + '.folder-' + subfolder

    @staticmethod
    def _is_folder_meta(objname: str):
        return objname.startswith('.folder-')

    def list_dir_raw(self, folder: str, include_dir: bool, page_size: int):
        for objname in self.list_dir_meta_folder(folder, page_size):
            if self._is_folder_meta(objname):
                if include_dir:
                    # Cut out the folder meta prefix
                    # and add /
                    yield objname[8:] + '/'
            else:
                yield objname

    def list_dir_meta_folder(self, folder: str, page_size: int):
        raise NotImplementedError()

    def is_dir_raw(self, folder: str) -> bool:
        return self.exists_raw(self._folder_meta_path(folder))

    def rmdir_raw(self, folder: str):
        return self.delete_raw(self._folder_meta_path(folder))

    def set_raw(self, key: str, value):
        key = self._ensure_slashes(key)[:-1]
        folder = key.rsplit('/', 1)[0]

        if folder:
            self.mkdir_raw(folder)
        self.folder_meta_set_raw(key, value)

    def folder_meta_set_raw(self, key: str, value):
        raise NotImplementedError()
