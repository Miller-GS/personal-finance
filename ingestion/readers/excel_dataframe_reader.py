import pandas as pd
import warnings
from readers.dataframe_reader import DataFrameReader

class ExcelDataFrameReader(DataFrameReader):
    def read_single(self, path: str) -> pd.DataFrame:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return pd.read_excel(path, index_col=None, header=0)
