from utils.utils import *
import graph.dag as dag

df = load_trace_data('', 'cella_job_hie')
print(f'{df.shape}')
dag.parse_trace(df, 'cella')