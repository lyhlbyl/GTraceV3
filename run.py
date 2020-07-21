import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx

def load_trace_data(fname):
    return pd.read_csv(f'data/{fname}.csv')

def demo_dag():
    G = nx.DiGraph()
    G.add_edges_from(
        [('A', 'B'), ('A', 'C'), ('D', 'B'), ('E', 'C'), ('E', 'F'),
         ('B', 'H'), ('B', 'G'), ('B', 'F'), ('C', 'G')])

    val_map = {'A': 1.0,
               'D': 0.5714285714285714,
               'H': 0.0}

    values = [val_map.get(node, 0.25) for node in G.nodes()]

    # Specify the edges you want here
    red_edges = [('A', 'C'), ('E', 'C')]
    edge_colours = ['black' if not edge in red_edges else 'red'
                    for edge in G.edges()]
    black_edges = [edge for edge in G.edges() if edge not in red_edges]

    # Need to create a layout when doing
    # separate calls to draw nodes and edges
    pos = nx.spring_layout(G)
    nx.draw_networkx_nodes(G, pos, cmap=plt.get_cmap('jet'),
                           node_color=values, node_size=500)
    nx.draw_networkx_labels(G, pos)
    nx.draw_networkx_edges(G, pos, edgelist=red_edges, edge_color='r', arrows=True)
    nx.draw_networkx_edges(G, pos, edgelist=black_edges, arrows=False)
    plt.show()

def demo_tree_plot():
    G = nx.DiGraph()

    G.add_node("ROOT")

    for i in range(5):
        G.add_node("Child_%i" % i)
        G.add_node("Grandchild_%i" % i)
        G.add_node("Greatgrandchild_%i" % i)

        G.add_edge("ROOT", "Child_%i" % i)
        G.add_edge("Child_%i" % i, "Grandchild_%i" % i)
        G.add_edge("Grandchild_%i" % i, "Greatgrandchild_%i" % i)

    # write dot file to use with graphviz
    # run "dot -Tpng test.dot >test.png"
    # nx.nx_agraph.write_dot(G, 'test.dot')

    # same layout using matplotlib with no labels
    plt.title('draw_networkx')
    pos = nx.nx_agraph.graphviz_layout(G, prog='dot')
    nx.draw(G, pos, with_labels=False, arrows=False)
    plt.show()

def demo_toy_tree():
    # dataset from https://rolandgeng.de/managing-trees-in-mysql-using-the-adjacency-list-model/
    df = load_trace_data('toy_tree')

    dag = nx.DiGraph()
    labels = {}
    for idx, r in df.iterrows():
        if len(str(r.title)) > 3:
            label = r.title[:3] + '.'
        else: label = r.title
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

if __name__ == '__main__':
    print("Gtrace Analysis")
    # print(df.head(10))
    demo_toy_tree()






