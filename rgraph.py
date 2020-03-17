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
import numpy as np
from collections import OrderedDict
import copy
import time

import redis
from redisgraph import Node, Edge, Graph

from absgraph import absgraph


# default port is 6379
class rgraph(absgraph):
    def __init__(self, server, schema):
        host, port = server.split(':')
        self._client_stub = redis.Redis(host=host, port=int(port))
        self._graph = Graph('test', self._client_stub)
        self._maxquery = 100
        self.cypher_no_exists = True

        super(rgraph, self).__init__(schema)

    def drop_all(self):
        self._graph.delete()

    def get_schema(self):
        pass

    def load_schema(self, schema):
        pass

    def set_index(self):
        for p, d in self.schema.items():
            if not 'index' in d:
                continue
            elif not d['index']:
                continue
            query = 'CREATE INDEX ON :node (%s)'%p
            self.query(query)

    def _is_intinstance(self, p, v):
        return self._int_type(p) and (isinstance(v, np.float_) or
                                     isinstance(v, float) or
                                     isinstance(v, str))

    def add_node(self, node):
        self._graph.add_node(node)

    def add_edge(self, edge):
        self._graph.add_edge(edge)
    
    def add_nodes(self, nodes):
        for i in nodes:
            node = Node(label='node', properties=i)
            self.add_node(node)

    def nquads(self, df, predicates, i):
        #d = self.id_predicate
        #nodes = [Node(label='node', properties={'%s'%d: df.iloc[i]['%s'%d],
        #    'numeric': srlz(df.iloc[i]['numeric'])}) for i in range(n, n+m)]
        plen = len(predicates)
        lim = min(i + self._maxquery, len(df))
        nquads = []
        while i < lim:
            properties = {}
            for p in predicates:
                try:
                    s = df.iloc[i][p]
                except:
                    s = i
                s = self.serialize(s)
                properties[p] = s
            nquads.append(properties)
            i += 1
        return nquads, i

    def nquads_edges(self, graph, label, i=0, nodes=None, neighbors=None, j=0):
        if nodes is None:
            nodes = list(graph.nodes())

        edges = []
        budget = self._maxquery
        for node in nodes[i:]:
            if neighbors is None:
                neighbors = list(graph.neighbors(node))
            for k, neigh in enumerate(neighbors[j:]):
                if budget == 0:
                    return edges, i, neighbors, j + k, self._maxquery
                edges.append((node, neigh))
                budget -= 1
            i += 1
            neighbors = None
            j = 0
        return edges, i, neighbors, j + k, self._maxquery - budget

    def nquads_edges2(self, edges):
        """
        GRAPH.QUERY test 'MATCH (a:node {name: "acc-tight5.mps.pkl__v998"}),
        (b:node {name: "acc-tight5.mps.pkl__v998"}), (c:node {name:
                "acc-tight5.mps.pkl__v10"}), (d:node {name:
                    "acc-tight5.mps.pkl__slack-min"}) CREATE (a)-[:edge]->(b),
                (c)-[:edge]->(d)'
        """
        # get all the nodes
        if isinstance(edges[0][0], str):
            base = '(s%d:node {%s: "%s"}), (d%d:node {%s: "%s"})'
        else:
            base = '(s%d:node {%s: %s}), (d%d:node {%s: %s})'
        l = []
        #q = 'MATCH '
        #for i, e in enumerate(edges):
        #    if i > 0:
        #        q += ', '
        #    q += base%(i, self.id_predicate, e[0], i, self.id_predicate, e[1])
        #    l.append('(s%d)-[:%s]->(d%d)'%(i, self.edge_attribute, i))
        #q += ' CREATE ' + ', '.join(l)
        m = []
        for i, e in enumerate(edges):
            m.append(base%(i, self.id_predicate, e[0], i, self.id_predicate,
                    e[1]))
            l.append('(s%d)-[:%s]->(d%d)'%(i, self.edge_attribute, i))
        q = 'MATCH ' + ', '.join(m) + ' CREATE ' + ', '.join(l)
        return q

    def parse_neighbors(self, res, ret):
        if len(res.result_set) == 0:
                return
        p = res.result_set[0][0].decode().split('.')[1] # XXX
        for k, v in res.result_set[1:]:
            k = k.decode()
            v = v.decode()
            if self._is_intinstance(p, k):
                k = int(float(k))
            if self._is_intinstance(p, v):
                v = int(float(v))
            if k in ret:
                ret[k].append(v)
            else:
                ret[k] = [v]

    def neighbors(self, identities, pred=None, id_pred=None):
        """
        We want something like
        GRAPH.QUERY test
        'MATCH (n: node {name:"acc-tight5.mps.pkl__v998"})-[:edge]->(m)
        RETURN m.numeric, m.name'

        or better solution like:

        GRAPH.QUERY test
        'MATCH (n0: node)-[:edge]->(m)
           WHERE n0.name = "acc-tight5.mps.pkl__v998"
           OR n0.name = "acc-tight5.mps.pkl__v10"
         RETURN n0.name, m.name'
        """
        if not id_pred:
            id_pred = self.id_predicate
        if not pred:
            pred = self.edge_attribute

        ret = OrderedDict()
        #
        # For small numbers, this is faster
        #
        if len(identities) < 1:
            for i in identities:
                q = 'MATCH (s: node {%s: "%s"})-[:%s]->(d) RETURN d.%s' % (
                        id_pred, i, pred, id_pred
                    )
                res = self.query(q)
                ret.update({i: [j[0].decode()
                        for j in res.result_set[1:]]})
            return ret
        #
        # Otherwise batch requests
        #
        if isinstance(identities[0], str):
            where_base = 'n.%s = "%s"'
        else:
            where_base = 'n.%s = %s'

        step = 8
        l = 0
        h = step
        lim = len(identities)
        ret = OrderedDict()
        while l < lim:
            b = identities[l:h]
            #where = ['n.%s = "%s"'%(id_pred, i) for i in b]
            where = [where_base%(id_pred, i) for i in b]
            where = ' OR '.join(where)
            query = (
                    'MATCH (n: node)-[:%s]->(m) WHERE %s RETURN n.%s, m.%s'
                    % (pred, where, id_pred, id_pred)
                    )
            res = self.query(query)
            self.parse_neighbors(res, ret)
            l = h
            h = h + step

        return ret

    def _int_type(self, predicate):
        return self.schema[predicate]['type'] == 'int'

    def parse_batch(self, res, ret, predicates):
        r = res.result_set[1]
        npreds = len(predicates)
        for i, p in enumerate(predicates):
            l = [j.decode() for j in r[i::npreds]]
            t = self.deserialize_type(p)
            if t:
                l = self.deserialize(l, t)
            # XXX
            if self._int_type(p) and (isinstance(l[0], np.float_) or
                                     isinstance(l[0], float) or
                                     isinstance(l[0], str)):
                l = [int(float(i)) for i in l]
            #if p in ret:
            #    ret[p].extend(l)
            #else:
            #    ret[p] = l
            ret[p].extend(l)

    def batch(self, identities, predicates, identities_predicate=None):
        if identities_predicate is None:
            identities_predicate = self.id_predicate

        if isinstance(identities[0], str):
            mtch_base = '(n%d:node {%s: "%s"})'
        else:
            mtch_base = '(n%d:node {%s: %s})'

        step = 1000
        l = 0
        h = step
        lim = len(identities)
        ret = OrderedDict({p: [] for p in predicates})
        while l < lim:
            nodes = identities[l:h]
            mtch = [mtch_base%(j, identities_predicate, i) for j, i
                    in enumerate(nodes)]
            mtch = ', '.join(mtch)
            rtrn = ['n%d.%s'%(j, p) for j in range(len(nodes)) for p in
                    predicates]
            rtrn = ', '.join(rtrn)
            query = 'MATCH ' + mtch + ' RETURN ' + rtrn
            res = self.query(query)
            # We get something like
            # [[b'n0.numeric', b'n0.name', b'n1.numeric', b'n1.name'], ...
            # res.result_set[1][0] is like b'xazf'

            self.parse_batch(res, ret, predicates)
            l = h
            h = h + step
        return ret

    def missing_values(self, predicate, low, high):
        return []

    def _one_cypher(self, predicate, identity):
        #query = 'MATCH (n:node {%s: "%s"}) RETURN n.%s'%(
        #        self.sorted_predicate, identity, predicate)
        if identity:
            if isinstance(identity, str):
                query = 'MATCH (n:node) WHERE n.%s = "%s" RETURN n.%s'%(
                    self.id_predicate, identity, predicate)
            else:
                query = 'MATCH (n:node) WHERE n.%s = "%s" RETURN n.%s'%(
                    self.id_predicate, identity, predicate)
        else:
            if self.cypher_no_exists:
                whr = 'n.%s != ""'
            else:
                whr = 'exists(n.%s)'
            query = ('MATCH (n:node) WHERE ' + whr + ' RETURN n.%s LIMIT 1')%(
                    predicate, predicate)
        return query

    def parse_one(self, res, predicate):
        return res.result_set[1][0].decode()

    def one(self, predicate, identity=None):
        query = self._one_cypher(predicate, identity)
        res = self.query(query)
        r = self.parse_one(res, predicate)
        t = self.deserialize_type(predicate)
        if t:
            r = self.deserialize([r], t)[0]
        return r

    def _count_cypher(self, name=None):
        if name is None:
            name = self.id_predicate
        return 'MATCH (n:node) RETURN COUNT(n)'

    def parse_count(self, res):
        return (int(float(res.result_set[1][0].decode())))

    def count(self, name=None):
        query = self._count_cypher(name)
        res = self.query(query)
        return self.parse_count(res)

    def merge(self):
        query = ''
        for _, node in self._graph.nodes.items():
            query += str(node) + ','

        for edge in self._graph.edges:
            query += str(edge) + ','

        # Discard leading comma.
        if query[-1] is ',':
            query = query[:-1]
        self._graph.merge(query)

    def commit(self):
        self._graph.commit()

    def flush(self):
        self._graph.flush()

    def query(self, query):
        return self._graph.query(query)

    def range_cypher(self, low, high, predicates, id_predicate, expand):
        unsortable = False
        if id_predicate is None:
            id_predicate = self.id_predicate
            if self.id_predicate_unsortable:
                unsortable = True

        rtrn = ['n.%s'%p for p in predicates]
        rtrn = ', '.join(rtrn)
        if unsortable:
            query = (
                    'MATCH (n: node) RETURN %s ORDER BY n.%s LIMIT %d'%
                     (rtrn, self.sorted_predicate, high - low)
                    )
        else:
            pred = self.sorted_predicate
            query = (
                     'MATCH (n: node) WHERE n.%s >= %d AND n.%s < %d '
                     'RETURN %s ORDER BY n.%s'%
                      (pred, low, pred, high, rtrn, pred)
                    )
        return query

    def _range_xform(self, ret, predicates):
        ret2 = [ {} for i in range(len(ret[predicates[0]])) ]
        for k, vs in ret.items():
            for j, v in enumerate(vs):
                ret2[j][k] = v
        return ret2

    def _range(self, low, high, predicates, id_predicate=None, expand=False):
        query = self.range_cypher(low, high, predicates, id_predicate, expand)
        ret = OrderedDict()
        res = self.query(query)
        res_predicates = [p.decode().split('.')[1] for p in res.result_set[0]]

        for pi, p in enumerate(predicates):
            tmp = [i[pi].decode() for i in res.result_set[1:]]
            ret[p] = tmp
        # for RETURN n.numeric, n.name, n.identity, ret[1:] is like
        # [[b'numericvalue0', b'namevalue0', b'identityvalue0'], ..]
        # Now ret looks like [{'name': [name values]}, {'numeric': []} ..]
        # To be compatible with _dataframe, we transform this to 
        # [{'name': 'namevalue0', 'numeric': 'numericvalue0'}, {}, {} ... ]
        ret2 = self._range_xform(ret, predicates)
        return ret2
                

    def load_df(self, df, predicates, n=0):
        print('loading nodes')
        while n < len(df):
            nquads, n = self.nquads(df, predicates, n)
            self.add_nodes(nquads)
            self.flush()

    def load_graph(self, g, edge):
        nodes = list(g.nodes())
        print('loading edges')
        n = 0
        j = 0
        nbrs = None
        num_nodes = len(nodes)
        n_prev = 0
        while n < num_nodes:
            nquads, n, nbrs, j, c = self.nquads_edges(g, edge, n,
                                      nodes=nodes, neighbors=nbrs, j=j)
            query = self.nquads_edges2(nquads)
            self.query(query)
            if n > n_prev + 10000:
                print('%d / %d'%(n, num_nodes))
                n_prev = n

if __name__ == '__main__':
    pass
