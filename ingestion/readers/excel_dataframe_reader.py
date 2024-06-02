import pandas as pd
from readers.dataframe_reader import DataFrameReader

class ExcelDataFrameReader(DataFrameReader):
    def read_single(self, path: str) -> pd.DataFrame:
        return pd.read_excel(path, index_col=None, header=0)
