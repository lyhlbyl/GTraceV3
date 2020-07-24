import sys
from utils.utils import *
import graph.dag as dag

if __name__ == "__main__":

    _ , arg = sys.argv
    df = load_trace_data('', f'cell{arg}_job_hie')
    print(f'{df.shape}')
    dag.parse_trace(df, 'cella')
