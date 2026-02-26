from abc import ABC, abstractmethod

class BaseService(ABC):
    @abstractmethod
    def DownloadFile(self, prefix, local_path):
        pass

    @abstractmethod
    def UploadFile(self, prefix, local_path):
        pass

    @abstractmethod
    def UploadFolder(self, prefix, local_path):
        pass

    @abstractmethod
    def DownloadFolder(self, prefix, local_path):
        pass

    @abstractmethod
    def IsFileExists(self, prefix):
        pass

    @abstractmethod
    def ListFiles(self, prefix, recursive=False):
        pass