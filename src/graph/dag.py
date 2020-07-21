from utils import  *
import matplotlib.pyplot as plt
import networkx as nx
from statistics import mean


debug_info = True

# TODO: other metrics from trace data: running duration, priority, verticle-scaling, only for root


def is_not_leave(item):
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
    max_depth = nx.dag_longest_path_length(dag)
    avg_depth = nx.average_shortest_path_length(dag)
    avg_clcoef = nx.average_clustering(dag)
    avg_degree = mean(map(prj_2nd, filter(is_not_leave, dag.out_degree())))


    # max_wdepth = max_depth
    # avg_wdepth = avg_depth
    if debug_info:
        print(
            '----------------------------------\n'
            f"Number of nodes: {nodes}\n"
            f"Max depth: {max_depth}, Avg depth: {avg_depth}\n"
            f"Average clustering coef: {avg_clcoef}\n"
            f"Average degree: {avg_degree}\n"
            '----------------------------------'
        )

    return nodes, max_depth, avg_depth, avg_clcoef, avg_degree

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


if __name__ == '__main__':
    print(__file__ )
    # toy_demo()

    dag = nx.DiGraph([(0,1), [0,2]])
    dag_metrics(dag)
