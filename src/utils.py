import pandas as pd

def load_trace_data(path_prefix, fname):
    return pd.read_csv(f'{path_prefix}data/{fname}.csv')

if __name__ == '__main__':
    print(__file__ )
