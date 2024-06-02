import pandas as pd
from abc import ABC, abstractmethod
from glob import glob

class DataFrameReader(ABC):
    @abstractmethod
    def read_single(self, path: str) -> pd.DataFrame:
        pass

    def read_with_glob_pattern(self, pattern: str) -> pd.DataFrame:
        all_files = glob(pattern)
        file_list = []
        for filename in all_files:
            df = self.read_single(filename)
            file_list.append(df)
        return pd.concat(file_list, axis=0, ignore_index=True)