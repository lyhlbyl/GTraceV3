import pprint
from graphviz import Digraph

from collections import defaultdict


class Vertex(object):
    def __init__(self, vid):
        self._id = vid

    def get_id(self): return self._id


class Edge(object):
    def __init__(self, eid, src, dst):
        self._id = eid
        # vertex id for the source node and the destined node
        self._src = src
        self._dst = dst

    def get_id(self): return self._id
    def get_src(self): return self._src
    def get_dst(self): return self._dst


class Graph(object):
    """ Graph data structure, DAG """
    def __init__(self, nodes, edges, gname = 'dag', directed=True):
        self._name = gname
        # dict for E and V
        self._E = defaultdict(Vertex)
        self._V = defaultdict(Edge)
        # dict for mappings
        self._G = defaultdict(set)

        for n in nodes:
            self.add_node(n)
        for e in edges:
            self.add_edge(e)

    def add_edge(self, edge):
        eid = edge.get_id()
        src = edge.get_src()
        dst = edge.get_dst()

        assert (src in self._V.keys())
        assert (dst in self._V.keys())
        assert (eid not in self._E.keys())
        self._E[eid] = edge
        self._G[src].add(dst)

    def add_node(self, node):
        vid = node.get_id()
        assert (vid not in self._V.keys())
        self._V[vid] = node

    def is_neighbor(self, nid1, nid2):
        return nid1 in self._G.keys and nid2 in self._G[nid1]

    def find_path(self, src, tgt):
        """ Find all path between node1 and node2  """
        if src == tgt:
            return [[src]]

        path = []
        for node in self._G[src]:
            rec_path = self.find_path(node, tgt)
            if rec_path:
                for p in rec_path:
                    path.append([src] + p)
        if path == []:
            return None
        return path

    def pprint(self):
        pretty_print = pprint.PrettyPrinter()
        pretty_print.pprint(self._G)

    def viz(self):
        dot = Digraph()
        for nid in self._V.keys():
            dot.node(nid)
        for src in self._G.keys():
            for dst in self._G[src]:
                dot.edge(src, dst)

        dot.render(f'out/{self._name}.gv', view=True)

    def __str__(self):
        return '{}({})'.format(self.__class__.__name__, dict(self._G))


def graph_parser(mapping) -> Graph:
    nids = []
    nodes = []
    edges = []
    for src, dst in mapping:
        if src not in nids:
            nids.append(src)
            nodes.append(Vertex(src))
        if dst not in nids:
            nids.append(dst)
            nodes.append(Vertex(dst))
        eid = f'{src}{dst}'
        edges.append(Edge(eid, src, dst))
    return Graph(nodes, edges)



if __name__ == '__main__':
    dependency = [('A', 'B'), ('A', 'D'), ('B', 'C'), ('B', 'D'),
                   ('C', 'D'), ('E', 'F'), ('F', 'C')]
    g = graph_parser(dependency)
    g.pprint()

    path = g.find_path('A', 'D')
    print(path)

    g.viz()

