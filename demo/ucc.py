import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import collections
import altair as alt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from kneed import KneeLocator
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, adjusted_rand_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from datetime import datetime


data_path = '../data/ucc/'

# Preprocessing:
idN = 'collection_id'


#  Priority:
def prio_rank(r, colN):
    if r[colN] < 100:
        return 0  # free
    elif r[colN] < 116:
        return 1  # beb
    elif r[colN] < 120:
        return 2  # mid
    elif r[colN] < 360:
        return 3  # prod
    else:
        return 4  # monitoring

def norm_prio(df):
    # axis=1 apply to row, 0 for coloumn
    colN = 'prio'
    new_df = df[[colN]].copy()
    new_df['prio_tier'] = new_df.apply(lambda r: prio_rank(r, colN), axis=1)
    return new_df['prio_tier']


def load_job_usage(cellN):
    old_cols = [idN, 'task_num', 'max_cpus', 'max_mem']
    df_usage = pd.read_csv(f"{data_path}cell_{cellN}_collectinon_usage.csv")
    df0 = pd.read_csv(f"{data_path}cell{cellN}_usage_plus.csv")[old_cols]
    return pd.merge(df_usage, df0, on=idN, how='left').drop('Unnamed: 0', axis=1)


def load_job_info(cellN):
    df_cell_root = pd.read_csv(f"{data_path}analysis/cell{cellN}_root.csv")
    df_cell_details = pd.read_csv(f"{data_path}cell{cellN}_collection.csv")
    df_cell_details['prio_tier'] = norm_prio(df_cell_details)
    return pd.merge(df_cell_root, df_cell_details, on=idN, how='left').drop('Unnamed: 0', axis=1)


def load_job_tree(cellN):
    return pd.read_csv(f"{data_path}analysis/cell{cellN}_dag.csv").drop('Unnamed: 0', axis=1)


def load_job_metrics(cellN):
    cols = ['collection_id', 'root_collection_id',
            'duration', 'task_num', 'cpu_usage', 'memory_usage', 'max_cpus', 'max_mem',
            'page_cache', 'cpi', 'mapi',
            'prio', 'prio_tier', 'sch_class', 'vertical_scaling']
    df_usage = load_job_usage(cellN)
    df_info = load_job_info(cellN)
    return pd.merge(df_usage, df_info, on=idN, how='left')[cols]


if __name__ == "__main__":

    tick = datetime.now()
    # load all metrics data for jobs
    df_e = load_job_metrics('e')
    df_f = load_job_metrics('f')
    df_g = load_job_metrics('g')
    df_h = load_job_metrics('h')
    tock = datetime.now()
    diff = tock - tick    # the result is a datetime.timedelta object
    print(diff.total_seconds())
