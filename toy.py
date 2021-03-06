import numpy as np
import pandas as pd
import utils
import matplotlib.pyplot as plt
import networkx as nx

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


if __name__ == '__main__':
    print(__file__)
    df = pd.DataFrame([1,2])
    data = {'id': ['Tom', 'nick', 'krish', 'jack'], 'Age': [20, 21, 19, 18]}
    df = pd.DataFrame(data)







