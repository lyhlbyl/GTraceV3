from utils.utils import *
import matplotlib.pyplot as plt
import networkx as nx
from statistics import mean
import datetime
import numpy as np
import math
import pandas as pd

debug_info = True

id_colN = 'collection_id'
paid_colN = 'parent_collection_id'

# TODO: other metrics from trace data: running duration, priority, verticle-scaling, only for root
def nonzero_degree(item):
    _, degree = item
    return degree > 0


def prj_2nd(item):
    _, x2 = item
    return x2
#
# Compute a list of metrics from DAG:
# - number of nodes, max/mean depth, max/mean non-leaves degree,
# - clustering coefficient
def dag_metrics(dag:nx.DiGraph):
    nodes = dag.number_of_nodes()
    edges = dag.number_of_edges()
    # print(f"before calc depth @ {datetime.datetime.now()}")
    # topologies = list(dag.all_topological_sorts(dag))
    # path_len_list = map(len, topologies)
    # max_depth = max(path_len_list)
    # nx.dag_longest_path_length(dag)
    # avg_depth = mean(path_len_list)
    # nx.average_shortest_path_length(dag)
    print(f"before avg clustering depth @ {datetime.datetime.now()}")
    avg_clcoef = nx.average_clustering(dag)
    print(f"before calc degree @ {datetime.datetime.now()}")
    all_degrees = list(map(prj_2nd, filter(nonzero_degree, dag.out_degree())))
    max_degree = 0
    avg_degree = 0
    if len(all_degrees) > 0:
        max_degree = max(all_degrees)
        avg_degree = mean(all_degrees)

    # max_wdepth = max_depth
    # avg_wdepth = avg_depth
    if debug_info:
        print(
            '----------------------------------\n'
            f"Number of nodes: {nodes}, edges: {edges}\n"
            # f"Max depth: {max_depth}, Avg depth: {avg_depth}\n"
            f"Average clustering coef: {avg_clcoef}\n"
            f"Max degree: {max_degree}, Ave degree: {avg_degree}\n"
            '----------------------------------'
        )
    return {'nodes': nodes,
            'edges': edges,
            'avg_clcoef': avg_clcoef,
            'max_degree': max_degree,
            'avg_degree': avg_degree}

def plot(dag):
    pos = nx.nx_agraph.graphviz_layout(dag, prog='dot')
    nx.draw_networkx_nodes(dag, pos,
                       node_color='b',
                       node_size=500,
                       alpha=0.3)
    nx.draw_networkx_edges(dag, pos, edge_color='b')
    # nx.draw_networkx_labels(dag, pos, labels, font_size=16)
    plt.show()

def toy_demo():
    # dataset from https://rolandgeng.de/managing-trees-in-mysql-using-the-adjacency-list-model/
    df = load_trace_data('../../', 'toy_tree')

    dag = nx.DiGraph()
    labels = {}
    for idx, r in df.iterrows():
        if len(str(r.title)) > 3:
            label = r.title[:3] + '.'
        else:
            label = r.title
        dag.add_node(r.id, name=label)
    for idx, r in df.iterrows():
        if r.parent_id not in dag.nodes:
            dag.add_node(r.parent_id)
        dag.add_edge(r.parent_id, r.id)

    dag_metrics(dag)

    try:
        pos = nx.nx_agraph.graphviz_layout(dag, prog='dot')
    except ImportError:
        pos = nx.spring_layout(dag, iterations=20)

    labels = nx.get_node_attributes(dag, 'name')

    nx.draw_networkx_nodes(dag, pos,
                           node_color='b',
                           node_size=500,
                           alpha=0.1)
    nx.draw_networkx_edges(dag, pos, edge_color='b')
    nx.draw_networkx_labels(dag, pos, labels, font_size=16)
    plt.show()

    return dag


    # df = load_trace_data('../../', fname)
    # apps = df.groupby('collection_logical_name')


def parse_trace_to_dag(df):
    if debug_info:
        print(f"trace to dag, df shape: {df.shape}")
    dag = nx.DiGraph()
    for r in df.itertuples():
        dag.add_node(r.collection_id)
    for r in df.itertuples():
        if not math.isnan(r.parent_collection_id):
            if r.parent_collection_id not in dag.nodes:
                dag.add_node(r.parent_collection_id)

            dag.add_edge(r.parent_collection_id, r.collection_id)

    return dag


def parse_trace(df, name):
    # clname = 'collection_logical_name'
    # clns = df[clname].unique()
    # hies = df.groupby(clname)
    #
    # df = pd.DataFrame([],
    #                   columns=[clname, 'nodes',
    #                            'edges', 'avg_clcoef',
    #                            'max_degree', 'avg_degree'])
    #
    # for cln in clns:
    #     print(f'paring {cln}')
    #     hie = hies.get_group(cln)
    #     print(f'rows: {hie.shape}')
    #     dag = parse_trace_to_dag(hie)
    #     metrics = dag_metrics(dag)
    #     metrics[clname] = cln
    #     df = df.append(metrics, ignore_index=True)

    pa_df = df[df[paid_colN].isnull()]
    chd_df = df[~df[paid_colN].isnull()]
    metric_df = pd.DataFrame([], columns=[id_colN, 'nodes','edges', 'avg_clcoef', 'max_degree', 'avg_degree'])
    cnt = 0
    for row in pa_df.itertuples():
        if debug_info:
            cnt = cnt + 1
            print(f"root collection id: {row.collection_id}, cnt: {cnt}")
        chd_df, hie = get_child_trace(chd_df, df[df[id_colN] == row.collection_id])
        dag = parse_trace_to_dag(hie)
        metric = dag_metrics(dag)
        metric[id_colN] = row.collection_id
        metric_df = metric_df.append(metric, ignore_index=True)

    print(metric_df.describe())
    metric_df.to_csv(f'{name}.csv')


def remove_id(df, id)-> pd.DataFrame:
    return df[df['id'] != id]

def get_child_trace(from_df, to_df):
    # If all id in the dest df are not the parent id of any ones in the from.
    # We reach leaf nodes.
    if set(to_df[id_colN].values).isdisjoint(set(from_df[paid_colN].values)):
        return from_df, to_df
    else:
        pids = set(to_df[id_colN].values) & set(from_df[paid_colN].values)
        new_child_nodes = from_df[from_df[paid_colN].isin(pids)]
        new_from = from_df[~from_df[paid_colN].isin(pids)]
        new_to = to_df.append(new_child_nodes, ignore_index=True)
        return get_child_trace(new_from, new_to)


if __name__ == '__main__':
    print(__file__ )
    # toy_demo()

    # # a quick test
    # dag = nx.DiGraph([(0,1), [0,2]])
    # dag_metrics(dag)
    # plot(dag)


    # a sample collection id from cell a:
    # df = load_trace_data('../../', 'cella_job_hie')
    # hies = df.groupby('collection_logical_name')
    # dag = parse_trace_to_dag(df)
    # dag_metrics(dag)
    # print(f"before plot{datetime.datetime.now()}")
    # plot(dag)


    df = load_trace_data('../', 'cella_jobs_sample')
    print(df.shape)
    parse_trace(df, 'cella_sample')