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
from collections import OrderedDict
import networkx as nx
import nonx as nonx
import joblib
from absgraph import absgraph
#from dgl import DGLGraph

class LRU(OrderedDict):
    def __init__(self, maxsize=2500, *args, **kwds):
        self.maxsize=maxsize
        super().__init__(*args, **kwds)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            oldest = next(iter(self))
            del self[oldest]

class xgraph(absgraph):
    def __init__(self, server, schema, transform=None, module=nx, db=None):
        self._module = module
        self._maxquery = 100
        self._df = None
        self._db = db

        super(xgraph, self).__init__(schema, transform)

    def drop_all(self):
        self._graph = None

    def get_schema(self):
        pass

    def load_schema(self, schema):
        pass

    def set_index(self):
        pass

    def count(self, name=None):
        if self._db:
            return self._db.count(name)
        return self._graph.number_of_nodes()

    def nquads(self, df, predicates, i):
        pass

    def nquads_edges(self, graph, label, i=0, nodes=None, neighbors=None, j=0):
        pass

    def _cached(self, identities, predicates):
        noncached = []
        cached = OrderedDict({p: [] for p in predicates})
        for i in identities:
            if i in self._nodes_map:
                for j, p in enumerate(predicates):
                    cached[p].append(self._nodes_map[i][p])
            else:
                noncached.append(i)
        return cached, noncached

    # XXX so far cache all the predicates
    def _update_node_caches2(self, identities):
        all_preds = self.all_predicates()
        id_pred = self.id_predicate
        cached, noncached = self._cached(identities, all_preds)
        # fetch non-cached node features
        b = self._db.batch_ordered(noncached, all_preds, id_pred)
        # update cache
        for j, i in enumerate(b[id_pred]):
            new = OrderedDict()
            for p in all_preds:
                new[p] = b[p][j]
            self._nodes_map[i] = new

        return cached, b

    def _update_node_caches_get(self, identities, predicates):
        cached, b = self._update_node_caches2(identities)
        for p in predicates:
            cached[p].extend(b[p])
        return cached

    def _update_node_caches(self, identities):
        _, _ = _update_node_caches2(identities)
        return

    def neighbors(self, identities, hop=1, local=False,
                  pred=None, id_pred=None):
        r"""cache is valid only when we want local neighbors ('local'=True)
        specified by 'identities'.
        Multiple edge types are not supported. Only identities must be
        id_predicate.
        """
        pred = self.edge_attribute if pred is None else pred
        id_pred = self.id_predicate if id_pred is None else id_pred

        if self._db:
            # Unfortunately we have no good way to cache correct neighbors
            return self._db.neighbors(identities, hop=hop, local=local,
                    pred=pred, id_pred=id_pred)

        r = OrderedDict()
        for i in identities:
            r[i] = {n for n in self._graph.neighbors(i)}
        return r

    # XXX required_predicates is unsupported
    def batch(self, identities, predicates, identities_predicate=None,
              identities_sorted=False, required_predicates=[]):
        if identities_predicate is None:
            identities_predicate = self.id_predicate

        # XXX
        if identities_predicate != self.id_predicate:
            tmp = [k for k, v in self._nodes_map.items()
                    if v[self.sorted_predicate] in identities]
            identities = tmp

        cached = self._update_node_caches_get(identities, predicates)
        return cached

    def batch_old(self, identities, predicates, identities_predicate=None,
              identities_sorted=False, required_predicates=[]):

        if identities_predicate is None:
            identities_predicate = self.id_predicate

        if identities_predicate != self.id_predicate:
            tmp = [k for k, v in self._nodes_map.items()
                    if v[self.sorted_predicate] in identities]
            identities = tmp

        ret = OrderedDict({p: [] for p in predicates})
        nocache = []
        for i in identities:
            # check the cache
            if not self._nodes_map or not i in self._nodes_map:
                nocache.append(i)
                continue
            for j, p in enumerate(predicates):
        #        v = self._df[self._df[identities_predicate] == i][p].values[0]
        #        ret[p].append(v)
                ret[p].append(self._nodes_map[i][p])

        if not len(nocache) or self._db is None:
            return ret

        print('predicates', predicates, 'identities', len(identities),
                'non cached', len(nocache))

        # forward down, but cache for all the predicates
        all_predicates = self.all_predicates()
        ret2 = self._db.batch_ordered(nocache, all_predicates,
                        identities_predicate)

        # update cache
        for j, i in enumerate(ret2[self.id_predicate]):
            new = OrderedDict()
            for p in all_predicates:
                new[p] = ret2[p][j]
            self._nodes_map[i] = new

        # merge
        for p in predicates:
            ret[p].extend(ret2[p])
        return ret

    def missing_values(self, predicate, low, high):
        return []

    def one(self, predicate, identity=None):
        # TODO clean up and use cache
        if not identity:
            if self._df is not None:
                return self._df[predicate][0]
            elif self._db is not None:
                return self._db.one(predicate, identity)
            return None

        if self._df is not None:
            row = self._df[self._df[self.id_predicate] == identity]
            return row[predicate].values[0]
        elif self._db:
            return self._db.one(predicate, identity)
        return None

    def load_df(self, df, predicates, n=0):
        self._df = df

    def init_cache(self):
        all_predicates = self.all_predicates()
        # create cache
        if self._db is not None:
            df = self._db.dataframe(0, 10, all_predicates)
        elif self._df is not None:
            df = self._df
        d0 = df.to_dict('list', into=OrderedDict)

        if self._db is not None:
            #d = LRU({i:OrderedDict() for i in d0[self.id_predicate]})
            d = LRU()
            for i in d0[self.id_predicate]:
                d[i] = OrderedDict()
        elif self._df is not None:
            d = OrderedDict({i:OrderedDict() for i in d0[self.id_predicate]})

        for p in all_predicates:
            if not p in d0:
                d0[p] = [i for i in range(len(df))]
            #print(d0[self.id_predicate], d0[p])
            for i, v in zip(d0[self.id_predicate], d0[p]):
                d[i][p] = v
        self._nodes_map = d

    def load_graph(self, g, edge):
        self._graph = g
        self.edge_attribute = edge

    def _range(self, low, high, predicates, id_predicate=None, expand=False):
        pass

    def load_df_file(self, path, predicates):
        df = joblib.load(path)
        self.load_df(df, predicates)

    def load_graph_file(self, path, edge):
        try:
            g = self._module.read_adjlist(path)
        except:
            g = nx.read_adjlist(path)
            #g = DGLGraph(g)
            print('done')
        self.load_graph(g, edge)

    # override
    def dataframe(self, low, high, predicates):
        if self._df is not None:
            rows = self._df.iloc[low:high]
            return rows[predicates]
        elif self._db is not None:
            print('xgraph.dataframe', 'loading from db')
            return self._db.dataframe(low, high, predicates)
        print('neither _df or _db is set')
        return None

    def noisolate(self, nodes):
        isolates = self._module.isolates(self._graph)
        if len(list(isolates)) == 0:
            return True
        for n in nodes:
            if n in isolates:
                return False
        return True

import sys
if __name__ == '__main__':
    server = None
    predicates = ['identity', 'category', 'document_tokens']
    edge = 'cite'
    schema = {predicates[0]: {'type': 'int', 'index': 'int', '_id': True},
              predicates[1]: {'type': 'int'},
              predicates[2]: {'type': 'string', '_type': 'csr_matrix'},
                       edge: {'type': 'string', 'edge': True}
             }
