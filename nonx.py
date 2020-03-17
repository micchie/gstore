"""
       <NAME OF THE PROGRAM THIS FILE BELONGS TO>
	  
  File:     gstore
  Authors:  Michio Honda (micchie.gml@gmail.com)

NEC Laboratories Europe GmbH, Copyright (c) 2020, All rights reserved.  

       THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
 
       PROPRIETARY INFORMATION ---  

SOFTWARE LICENSE AGREEMENT

ACADEMIC OR NON-PROFIT ORGANIZATION NONCOMMERCIAL RESEARCH USE ONLY

BY USING OR DOWNLOADING THE SOFTWARE, YOU ARE AGREEING TO THE TERMS OF THIS
LICENSE AGREEMENT.  IF YOU DO NOT AGREE WITH THESE TERMS, YOU MAY NOT USE OR
DOWNLOAD THE SOFTWARE.

This is a license agreement ("Agreement") between your academic institution
or non-profit organization or self (called "Licensee" or "You" in this
Agreement) and NEC Laboratories Europe GmbH (called "Licensor" in this
Agreement).  All rights not specifically granted to you in this Agreement
are reserved for Licensor. 

RESERVATION OF OWNERSHIP AND GRANT OF LICENSE: Licensor retains exclusive
ownership of any copy of the Software (as defined below) licensed under this
Agreement and hereby grants to Licensee a personal, non-exclusive,
non-transferable license to use the Software for noncommercial research
purposes, without the right to sublicense, pursuant to the terms and
conditions of this Agreement. NO EXPRESS OR IMPLIED LICENSES TO ANY OF
LICENSOR'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. As used in this
Agreement, the term "Software" means (i) the actual copy of all or any
portion of code for program routines made accessible to Licensee by Licensor
pursuant to this Agreement, inclusive of backups, updates, and/or merged
copies permitted hereunder or subsequently supplied by Licensor,  including
all or any file structures, programming instructions, user interfaces and
screen formats and sequences as well as any and all documentation and
instructions related to it, and (ii) all or any derivatives and/or
modifications created or made by You to any of the items specified in (i).

CONFIDENTIALITY/PUBLICATIONS: Licensee acknowledges that the Software is
proprietary to Licensor, and as such, Licensee agrees to receive all such
materials and to use the Software only in accordance with the terms of this
Agreement.  Licensee agrees to use reasonable effort to protect the Software
from unauthorized use, reproduction, distribution, or publication. All
publication materials mentioning features or use of this software must
explicitly include an acknowledgement the software was developed by NEC
Laboratories Europe GmbH.

COPYRIGHT: The Software is owned by Licensor.  

PERMITTED USES:  The Software may be used for your own noncommercial
internal research purposes. You understand and agree that Licensor is not
obligated to implement any suggestions and/or feedback you might provide
regarding the Software, but to the extent Licensor does so, you are not
entitled to any compensation related thereto.

DERIVATIVES: You may create derivatives of or make modifications to the
Software, however, You agree that all and any such derivatives and
modifications will be owned by Licensor and become a part of the Software
licensed to You under this Agreement.  You may only use such derivatives and
modifications for your own noncommercial internal research purposes, and you
may not otherwise use, distribute or copy such derivatives and modifications
in violation of this Agreement.

BACKUPS:  If Licensee is an organization, it may make that number of copies
of the Software necessary for internal noncommercial use at a single site
within its organization provided that all information appearing in or on the
original labels, including the copyright and trademark notices are copied
onto the labels of the copies.

USES NOT PERMITTED:  You may not distribute, copy or use the Software except
as explicitly permitted herein. Licensee has not been granted any trademark
license as part of this Agreement.  Neither the name of NEC Laboratories
Europe GmbH nor the names of its contributors may be used to endorse or
promote products derived from this Software without specific prior written
permission.

You may not sell, rent, lease, sublicense, lend, time-share or transfer, in
whole or in part, or provide third parties access to prior or present
versions (or any parts thereof) of the Software.

ASSIGNMENT: You may not assign this Agreement or your rights hereunder
without the prior written consent of Licensor. Any attempted assignment
without such consent shall be null and void.

TERM: The term of the license granted by this Agreement is from Licensee's
acceptance of this Agreement by downloading the Software or by using the
Software until terminated as provided below.  

The Agreement automatically terminates without notice if you fail to comply
with any provision of this Agreement.  Licensee may terminate this Agreement
by ceasing using the Software.  Upon any termination of this Agreement,
Licensee will delete any and all copies of the Software. You agree that all
provisions which operate to protect the proprietary rights of Licensor shall
remain in force should breach occur and that the obligation of
confidentiality described in this Agreement is binding in perpetuity and, as
such, survives the term of the Agreement.

FEE: Provided Licensee abides completely by the terms and conditions of this
Agreement, there is no fee due to Licensor for Licensee's use of the
Software in accordance with this Agreement.

DISCLAIMER OF WARRANTIES:  THE SOFTWARE IS PROVIDED "AS-IS" WITHOUT WARRANTY
OF ANY KIND INCLUDING ANY WARRANTIES OF PERFORMANCE OR MERCHANTABILITY OR
FITNESS FOR A PARTICULAR USE OR PURPOSE OR OF NON- INFRINGEMENT.  LICENSEE
BEARS ALL RISK RELATING TO QUALITY AND PERFORMANCE OF THE SOFTWARE AND
RELATED MATERIALS.

SUPPORT AND MAINTENANCE: No Software support or training by the Licensor is
provided as part of this Agreement.  

EXCLUSIVE REMEDY AND LIMITATION OF LIABILITY: To the maximum extent
permitted under applicable law, Licensor shall not be liable for direct,
indirect, special, incidental, or consequential damages or lost profits
related to Licensee's use of and/or inability to use the Software, even if
Licensor is advised of the possibility of such damage.

EXPORT REGULATION: Licensee agrees to comply with any and all applicable
export control laws, regulations, and/or other laws related to embargoes and
sanction programs administered by law.

SEVERABILITY: If any provision(s) of this Agreement shall be held to be
invalid, illegal, or unenforceable by a court or other tribunal of competent
jurisdiction, the validity, legality and enforceability of the remaining
provisions shall not in any way be affected or impaired thereby.

NO IMPLIED WAIVERS: No failure or delay by Licensor in enforcing any right
or remedy under this Agreement shall be construed as a waiver of any future
or other exercise of such right or remedy by Licensor.

GOVERNING LAW: This Agreement shall be construed and enforced in accordance
with the laws of Germany without reference to conflict of laws principles.
You consent to the personal jurisdiction of the courts of this country and
waive their rights to venue outside of Germany.

ENTIRE AGREEMENT AND AMENDMENTS: This Agreement constitutes the sole and
entire agreement between Licensee and Licensor as to the matter set forth
herein and supersedes any previous agreements, understandings, and
arrangements between the parties relating hereto.

       THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.

"""
from collections import Mapping, Set, Iterable

#def parse_adjlist(lines, comments='#', delimiter=None,
#                  create_using=None, nodetype=None):
#    G = nx.empty_graph(0, create_using)
#    for line in lines:
#        p = line.find(comments)
#        if p >= 0:
#            line = line[:p]
#        if not len(line):
#            continue
#        vlist = line.strip().split(delimiter)
#        u = vlist.pop(0)
#        # convert types
#        if nodetype is not None:
#            try:
#                u = nodetype(u)
#            except:
#                raise TypeError("Failed to convert node ({}) to type {}"
#                                .format(u, nodetype))
#        G.add_node(u)
#        if nodetype is not None:
#            try:
#                vlist = map(nodetype, vlist)
#            except:
#                raise TypeError("Failed to convert nodes ({}) to type {}"
#                                .format(','.join(vlist), nodetype))
#        G.add_edges_from([(u, v) for v in vlist])
#    return G
#
#@open_file(0, mode='rb')
#def read_adjlist(path, comments="#", delimiter=None, create_using=None,
#                 nodetype=None, encoding='utf-8'):
#    lines = (line.decode(encoding) for line in path)
#    return parse_adjlist(lines,
#                         comments=comments,
#                         delimiter=delimiter,
#                         create_using=create_using,
#                         nodetype=nodetype)
#
# Fake networkx that exports only subset of the interfaces
# The main purpose of this software is to clarify necessary interfaces
# for greph representation learning
#
def set_node_attributes(G, values, name=None):
    # print(type(values), values)
    # something like <class 'dict'> {1: 6, 2: 6, 3: 1 ...
    if name is not None:  # `values` must not be a dict of dict
        try:  # `values` is a dict
            for n, v in values.items():
                try:
                    G.nodes[n][name] = values[n]
                except KeyError:
                    pass
        except AttributeError:  # `values` is a constant
            for n in G:
                G.nodes[n][name] = values
    else:  # `values` must be dict of dict
        for n, d in values.items():
            try:
                G.nodes[n].update(d)
            except KeyError:
                pass

def _plain_bfs(G, source):
    """A fast BFS node generator"""
    G_adj = G.adj
    seen = set()
    nextlevel = {source}
    while nextlevel:
        thislevel = nextlevel
        nextlevel = set()
        for v in thislevel:
            if v not in seen:
                yield v
                seen.add(v)
                nextlevel.update(G_adj[v])

def connected_components(G):
    seen = set()
    for v in G:
        if v not in seen:
            c = set(_plain_bfs(G, v))
            yield c
            seen.update(c)

class show_nodes(object):
    def __init__(self, nodes):
        self.nodes = set(nodes)

    def __call__(self, node):
        return node in self.nodes

class DegreeView(object):
    def __init__(self, G):
        self._graph = G
        self._succ = G._adj
        self._pred = G._adj
        self._nodes = self._succ

    def __iter__(self):
        for n in self._nodes:
            nbrs = self._succ[n]
            yield (n, len(nbrs) + (n in nbrs))

class AtlasView(Mapping):
    """An AtlasView is a Read-only Mapping of Mappings.

    It is a View into a dict-of-dict data structure.
    The inner level of dict is read-write. But the
    outer level is read-only.

    See Also
    ========
    AdjacencyView - View into dict-of-dict-of-dict
    MultiAdjacencyView - View into dict-of-dict-of-dict-of-dict
    """
    __slots__ = ('_atlas',)

    def __init__(self, d):
        self._atlas = d

    def __len__(self):
        return len(self._atlas)

    def __iter__(self):
        return iter(self._atlas)

    def __getitem__(self, key):
        return self._atlas[key]

    def copy(self):
        return {n: self[n].copy() for n in self._atlas}

    def __str__(self):
        return str(self._atlas)  # {nbr: self[nbr] for nbr in self})

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._atlas)

class AdjacencyView(AtlasView):
    """An AdjacencyView is a Read-only Map of Maps of Maps.

    It is a View into a dict-of-dict-of-dict data structure.
    The inner level of dict is read-write. But the
    outer levels are read-only.

    See Also
    ========
    AtlasView - View into dict-of-dict
    MultiAdjacencyView - View into dict-of-dict-of-dict-of-dict
    """
    __slots__ = ()   # Still uses AtlasView slots names _atlas

    def __getitem__(self, name):
        return AtlasView(self._atlas[name])

    def copy(self):
        return {n: self[n].copy() for n in self._atlas}

class NodeView(Mapping, Set):
    __slots__ = '_nodes',

    def __init__(self, graph):
        self._nodes = graph._node

    def __iter__(self):
        return iter(self._nodes)

    # Mapping methods
    def __len__(self):
        return len(self._nodes)

    def __getitem__(self, n):
        return self._nodes[n]

    def __contains__(self, n):
        return n in self._nodes

    def __call__(self):
        return self


def freeze(G):
    return G

class FilterAdjacency(Mapping):   # edgedict
    def __init__(self, d, NODE_OK, EDGE_OK):
        self._atlas = d
        self.NODE_OK = NODE_OK
        self.EDGE_OK = EDGE_OK

    def __len__(self):
        return sum(1 for n in self)

    def __iter__(self):
        return (n for n in self._atlas if self.NODE_OK(n))

class FilterMultiInner(FilterAdjacency):  # muliedge_seconddict
    def __iter__(self):
        my_nodes = (n for n in self._atlas if self.NODE_OK(n))
        for n in my_nodes:
            some_keys_ok = False
            for key in self._atlas[n]:
                if self.EDGE_OK(n, key):
                    some_keys_ok = True
                    break
            if some_keys_ok is True:
                yield n

    def __getitem__(self, nbr):
        if nbr in self._atlas and self.NODE_OK(nbr):
            def new_node_ok(key):
                return self.EDGE_OK(nbr, key)
            return FilterAtlas(self._atlas[nbr], new_node_ok)
        raise KeyError("Key {} not found".format(nbr))

    def copy(self):
        return {v: {k: d for k, d in nbrs.items() if self.EDGE_OK(v, k)}
                for v, nbrs in self._atlas.items() if self.NODE_OK(v)}


class FilterMultiAdjacency(FilterAdjacency):  # multiedgedict
    def __getitem__(self, node):
        if node in self._atlas and self.NODE_OK(node):
            def edge_ok(nbr, key):
                return self.NODE_OK(nbr) and self.EDGE_OK(node, nbr, key)
            return FilterMultiInner(self._atlas[node], self.NODE_OK, edge_ok)
        raise KeyError("Key {} not found".format(node))

    def copy(self):
        return {u: {v: {k: d for k, d in kd.items()
                        if self.EDGE_OK(u, v, k)}
                    for v, kd in nbrs.items() if self.NODE_OK(v)}
                for u, nbrs in self._atlas.items() if self.NODE_OK(u)}


class FilterAtlas(Mapping):  # nodedict, nbrdict, keydict
    def __init__(self, d, NODE_OK):
        self._atlas = d
        self.NODE_OK = NODE_OK

    def __len__(self):
        return sum(1 for n in self)

    def __iter__(self):
        return (n for n in self._atlas if self.NODE_OK(n))

    def __getitem__(self, key):
        if key in self._atlas and self.NODE_OK(key):
            return self._atlas[key]
        raise KeyError("Key {} not found".format(key))

    def copy(self):
        return {u: d for u, d in self._atlas.items()
                if self.NODE_OK(u)}

    def __repr__(self):
        return '%s(%r, %r)' % (self.__class__.__name__, self._atlas,
                               self.NODE_OK)

def subgraph_view(G, filter_node=None, filter_edge=None):
    newG = freeze(G.__class__())
    newG._NODE_OK = filter_node
    newG._EDGE_OK = filter_edge

    newG._graph = G
    newG.graph = G.graph

    newG._node = FilterAtlas(G._node, filter_node)
    Adj = FilterMultiAdjacency
    newG._adj = Adj(G._adj, filter_node, filter_edge)
    return newG


class Graph(object):

    node_dict_factory = dict
    adjlist_outer_dict_factory = dict
    adjlist_inner_dict_factory = dict
    edge_attr_dict_factory = dict

    def __init__(self):
        self.node_dict_factory = ndf = self.node_dict_factory
        self.adjlist_outer_dict_factory = self.adjlist_outer_dict_factory
        self.graph = {}
        self._node = ndf()
        self._adj = self.adjlist_outer_dict_factory()

    def __iter__(self):
        return iter(self._node)

    def __contains__(self, n):
        try:
            return n in self._node
        except TypeError:
            return False

    def add_nodes_from(self, nodes_for_adding, **attr):
        for n in nodes_for_adding:
            # keep all this inside try/except because
            # CPython throws TypeError on n not in self._node,
            # while pre-2.7.5 ironpython throws on self._adj[n]
            try:
                if n not in self._node:
                    self._adj[n] = self.adjlist_inner_dict_factory() # just dict
                    self._node[n] = attr.copy()
                else:
                    self._node[n].update(attr)
            except TypeError:
                nn, ndict = n
                if nn not in self._node:
                    self._adj[nn] = self.adjlist_inner_dict_factory()
                    newdict = attr.copy()
                    newdict.update(ndict)
                    self._node[nn] = newdict
                else:
                    olddict = self._node[nn]
                    olddict.update(attr)
                    olddict.update(ndict)

    def add_edges_from(self, ebunch_to_add, **attr):
        for e in ebunch_to_add:
            # e is a tuple of strings e.g., ('A', 'B')
            ne = len(e)
            if ne == 3:
                u, v, dd = e
            elif ne == 2:
                u, v = e
                dd = {}  # doesn't need edge_attr_dict_factory
            else:
                raise Exception(
                    "Edge tuple %s must be a 2-tuple or 3-tuple." % (e,))
            if u not in self._node:
                self._adj[u] = self.adjlist_inner_dict_factory()
                self._node[u] = {}
            if v not in self._node:
                self._adj[v] = self.adjlist_inner_dict_factory()
                self._node[v] = {}
            datadict = self._adj[u].get(v, self.edge_attr_dict_factory())
            datadict.update(attr)
            datadict.update(dd)
            self._adj[u][v] = datadict
            self._adj[v][u] = datadict

    def number_of_nodes(self):
        return len(self._node)

    def size(self, weight=None):
        s = sum(d for v, d in self.degree())
        return s // 2

    def number_of_edges(self):
        return int(self.size())

    def degree(self):
        return DegreeView(self)

    def nodes(self):
        nodes = NodeView(self)
        self.__dict__['nodes'] = nodes
        return nodes

    def subgraph(self, nodes):
        induced_nodes = show_nodes(self.nbunch_iter(nodes))
        subgraph = subgraph_view
        if hasattr(self, '_NODE_OK'):
            return subgraph(self._graph, induced_nodes, self._EDGE_OK)
        return subgraph(self, induced_nodes)

    def nbunch_iter(self, nbunch=None):
        if nbunch is None:
            bunch = iter(self._adj)
        elif nbunch in self:
            bunch = iter([nbunch])
        else:
            def bunch_iter(nlist, adj):
                try:
                    for n in nlist:
                        if n in adj:
                            yield n
                except TypeError as e:
                    message = e.args[0]
                    # capture error for non-sequence/iterator nbunch.
                    if 'iter' in message:
                        msg = "nbunch is not a node or a sequence of nodes."
                        raise Exception(msg)
                    # capture error for unhashable node.
                    elif 'hashable' in message:
                        msg = "Node {} in sequence nbunch is not a valid node."
                        raise Exception(msg.format(n))
                    else:
                        raise
            bunch = bunch_iter(nbunch, self._adj)
        return bunch
    def is_multigraph(self):
        return False

    def is_directed(self):
        return False
    
    @property
    def adj(self):
        return AdjacencyView(self._adj)

    def neighbors(self, n):
        try:
            return iter(self._adj[n])
        except KeyError:
            raise Exception("The node %s is not in the graph." % (n,))


def isolates(G):
    return (n for n, d in G.degree() if d == 0)

if __name__ == '__main__':

    graph = Graph()
    
    nodes = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
    graph.add_nodes_from(nodes)
    print(graph.nodes)
    
    edges = [
        ('A', 'B'),
        ('A', 'C'),
        ('B', 'D'),
        ('B', 'F'),
        ('C', 'D'),
        ('C', 'E'),
        ('F', 'G'),
        ('F', 'H'),
        ('G', 'H'),
        ('G', 'I')
    ]
    
    graph.add_edges_from(edges)
    print(graph.edges)
