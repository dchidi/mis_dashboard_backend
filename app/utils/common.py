import pandas as pd


def column_separator(table1, table2):
    return pd.DataFrame({"": [""] * max(len(table1), len(table2))})
