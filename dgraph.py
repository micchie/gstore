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

import pydgraph
import json
from collections import OrderedDict
import copy

import pandas as pd
import sys
import pickle

import networkx as nx
import numpy

import torch
torch.set_printoptions(profile='default') # use full for debug

#def connect_dgraph():
#    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#    client.connect(("localhost", 5080))
#    client.send(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
#    response = client.recv(4096)
#    print("response", response)
#

from absgraph import absgraph

# default port is 9080
class dgraph(absgraph):
    def __init__(self, server, schema, transform=None):
        self.server = server
        self._client_stub = pydgraph.DgraphClientStub(server)
        self._client = pydgraph.DgraphClient(self._client_stub)
        self._maxquery = 1000
        self.uidmap = {}

        super(dgraph, self).__init__(schema, transform)

    def drop_all(self):
        op = pydgraph.Operation(drop_all=True)
        self._client.alter(op)

    #
    # Example
    #
    #predicates = ['name', 'numeric', 'identity']
    #edge = 'edge'
    #schema = {predicates[0]: {'type': 'string', 'index': 'hash'},
    #          predicates[1]: {'type': 'string'},
    #          predicates[2]: {'type': 'int', 'index': 'int'},
    #                   edge: {'type': 'string', 'edge': True}
    #         }
    def get_schema(self):
        query = 'schema {type index count tokenizer reverse}'
        res = self.query(query)
        d = {}
        for i in res.schema:
            if hasattr(i, 'predicate'):
                if i.predicate == '_predicate_':
                    continue
                d[i.predicate] = {}
            for a in ['type', 'index', 'count', 'tokenizer', 'reverse']:
                if hasattr(i, a):
                    d[i.predicate][a] = getattr(i, a)
        d2 = {}
        for k, v in d.items():
            newv = copy.deepcopy(v)
            if len(newv['tokenizer']) > 0:
                newv['index'] = newv['tokenizer'][0] # XXX
            del newv['tokenizer']
            d2[k] = newv
        print(d2)
        return d2

    def load_schema(self, d):
        schema = ''
        for k, v in d.items():
            schema += k + ': '
            if 'edge' in v:
                if v['edge'] == True:
                    schema += 'uid @count'
            else:
                schema += v['type']
                if 'index' in v:
                    schema += ' @index(%s)'%(v['index'])
            schema += ' .\n'
        self._schema(schema)
        return

    def set_index(self):
        pass

    def _schema(self, schema):
        #
        # Example:
        # schema = \
        # """
        # identity: int @index(int) .
        # category: int .
        # document_tokens: string .
        # cite: uid @count .
        # """
        #
        print(schema)
        op = pydgraph.Operation(schema=schema)
        self._client.alter(op)

    def uid(self, identities):
        #t = (self.id_predicate, str(identities))
        t = (self.id_predicate, json.dumps(identities), self.id_predicate)
        query = '{get_uid (func: eq(%s, %s)) {%s <uid>}}' % t
        res = self.query(query)
        d = json.loads(res.json.decode())

        if type(identities) is list:
            return d['get_uid']
        elif len(d['get_uid']) > 0:
            return d['get_uid'][0]['uid']
        return None

    def count(self, name=None):
        if name is None:
            name = self.id_predicate
        query = '{count(func: has(<%s>)) {total: count(uid)}}'%name
        res = self.query(query)
        d = json.loads(res.json.decode())
        return d['count'][0]['total']
    #
    # args; df, predicates (list) and df index (position) to start 
    # return: nquads and next df index
    #
    def nquads(self, df, predicates, i):
        plen = len(predicates)
        lim = min(i + int(self._maxquery / plen) + plen - 1, len(df))
        nquads = ''
        names = []
        while i < lim:
            names.append(df[predicates[0]][i])
            for p in predicates:
                try:
                    s = df.iloc[i][p]
                except:
                    s = i
                s = self.serialize(s)
                nquads += '_:%d <%s> "%s" .\n' % (i, p, s)
            i += 1
        return nquads, i, names

    def nquads_rdf_dataset(self, dataset, predicates, edge, i, lim, path):

        print('nquads_rdf_dataset')
        data = dataset[0]
        f = open(path, 'w')

        start = i
        i_prev = i
        while i < lim:
            nquads = ''
            for p in predicates:
                try:
                    o = data[p]
                    oi = o[i].clone().detach()
                except:
                    oi = i
                s = self.serialize(oi)
                nquads += '_:%d <%s> "%s" .\n' % (i, p, s)
            # neighbors
            #for e in data.edge_index.t():
            #    if e[0] == i:
            #        nquads += '_:%d <%s> _:%d .\n' % (e[0], edge, e[1])
            i += 1
            if i > i_prev + 10000:
                f.write(nquads)
                nquads = ''
                print('node %d / %d'%(i, lim))
                i_prev = i

        # neighbors
        nquads = ''
        i = start
        i_prev = start
        for e in data.edge_index.t():
            if e[0] < start or e[0] >= lim:
                continue
            nquads += '_:%d <%s> _:%d .\n' % (e[0], edge, e[1])
            i += 1
            if i > i_prev + 10000:
                print('edge %d'%i)
                i_prev = i
                f.write(nquads)
                nquads = ''
        f.write(nquads)
        f.close()


    def nquads_rdf(self, df, g, predicates, edge, i, lim, path):

        namemap = {}
        print('first, create node-index map')
        for j in range(i, lim):
            name = df.at[j, predicates[0]]
            idx = j
            namemap[name] = j
        print('done')

        f = open(path, 'w')

        i_prev = i
        while i < lim:
            nquads = ''
            for p in predicates:
                try:
                    s = df.iloc[i][p]
                except:
                    s = i
                s = self.serialize(s)
                nquads += '_:%d <%s> "%s" .\n' % (i, p, s)
            # neighbors
            me = df[predicates[0]][i] # name
            for nei in g.neighbors(me):
                df_i = namemap[nei]
                nquads += '_:%d <%s> _:%d .\n' % (i, edge, df_i)
            f.write(nquads)
            i += 1
            if i > i_prev + 10000:
                print('%d / %d'%(i, lim))
                i_prev = i

        f.close()

    def nquads_dataset(self, dataset, predicates, i):
        x = dataset[0].x # In PyG x is always a feature set
        plen = len(predicates)
        lim = min(i + int(self._maxquery / plen) + plen - 1, len(x))
        nquads = ''
        names = []
        while i < lim:
            names.append(i)
            for p in predicates:
                try:
                    o = getattr(dataset[0], p)
                    oi = o[i].clone().detach()
                except:
                    oi = i
                s = self.serialize(oi)
                nquads += '_:%d <%s> "%s" .\n' % (i, p, s)
            i += 1
        return nquads, i, names

    def nquads_edges(self, graph, label, i, nodes=None, neighbors=None, j=0):
        nquads = ''
        budget = self._maxquery

        if nodes is None:
            nodes = list(graph.nodes())

        for node in nodes[i:]:
            found = False
            try:
                suid = self.uidmap[node]
                found = True
            except:
                print('%s is not in uidmap'%node)

            if not found:
                if budget == self._maxquery:
                    print('creating %d src map'%budget)
                    r = self.uid(nodes[i:i+budget])
                    ks = [d[self.id_predicate] for d in r]
                    vs = [d['uid'] for d in r]
                    name_uid_map_src = {k: v for (k, v) in zip(ks, vs)}
                    print('src map for %d neighbors'%len(name_uid_map_src))

                try:
                    suid = name_uid_map_src[node]
                except:
                    print('%s is not in cache'%node)
                    suid = self.uid(node)

            if suid:
                if neighbors is None:
                    neighbors = list(graph.neighbors(node))

                for k, nei in enumerate(neighbors[j:]):
                    if budget == 0:
                        c = self._maxquery - budget
                        return nquads, i, neighbors, j + k,\
                                self._maxquery - budget
                    found = False
                    try:
                        duid = self.uidmap[nei]
                        found = True
                    except:
                        print('%s is not in uidmap'%nei)

                    if not found:
                        # create a local cache
                        # Note eq(name, ['x', 'y']) does not keep orders!
                        if k == 0:
                            print('creating nbr map for node ', node)
                            r = self.uid(neighbors[j:j+budget])
                            print('received uids')
                            ks = [d[self.id_predicate] for d in r]
                            vs = [d['uid'] for d in r]
                            name_uid_map = {k: v for (k, v) in zip(ks, vs)}
                            print('nbr map for %d neighbors'%len(name_uid_map))
                        try:
                            duid = name_uid_map[nei]
                        except:
                            print('%s is not in nbr map'%nei)
                            duid = self.uid(nei)

                    if duid is None:
                        print('nei %s has no uid'%nei)
                        continue
                    nquads += '<%s> <%s> <%s> .\n' % (suid, label, duid)
                    budget -= 1
            i += 1
            neighbors = None
            j = 0
        c = self._maxquery - budget
        #print('nquads_edges created %d nquads'%(self._maxquery - budget))
        return nquads, i, neighbors, j, self._maxquery - budget

    def mutate(self, nquads, lids=None, names=None):

        if lids or names:
            if len(lids) != len(names):
                raise Exception
        txn = self._client.txn()
        assigned = txn.mutate(set_nquads=nquads, commit_now=True)
        if lids:
            lids = [str(i) for i in lids]
            for li, name in zip(lids, names):
                self.uidmap.update({name:assigned.uids[li]})

    def query(self, nquads):
        txn = self._client.txn()
        return txn.query(nquads)

    def _range(self, low, high, predicates, id_predicate=None, expand=False):
        pl = ''
        for p in predicates:
            pl += '%s '%p

        unsortable = False
        if id_predicate is None:
            id_predicate = self.id_predicate
            if self.id_predicate_unsortable:
                unsortable = True

        odr = odr2 = fltr = ''
        if unsortable:
            odr = ', first: %s'%(high - low)
            if expand:
                odr2 = '{%s}'%(id_predicate)
        else:
            odr = ', orderasc: %s'%id_predicate
            if expand:
                odr2 = '(orderasc: %s) {%s}'%(id_predicate, id_predicate)
            fltr = '@filter(ge(%s, "%d") AND lt(%s, "%d"))' % \
                    (id_predicate, low, id_predicate, high)

        query = ('{x (func: has(%s)%s) %s {%s %s}}'
                %(id_predicate, odr, fltr, pl, odr2)
                )
        res = self.query(query) # res.json is bytes type
        d = json.loads(res.json.decode())
        return d['x']

    def _graph(self, low, high, edge):
        r = self._range(low, high, [self.id_predicate, edge], expand=True)
        adjlist = {}
        for i in r:
            adjlist[i[self.id_predicate]] = [
                    j[self.id_predicate] for j in i[edge]
            ]
        return adjlist

    def graph(self, low, high, edge, graphlib=nx):
        graph = graphlib.Graph()
        adjlists = {}
        while low < high:
            partial = self._graph(low, low + self._maxquery, edge)
            adjlists.update(partial)
            low += self._maxquery
        nodes = [i for i in adjlists.keys()]
        edges = []
        for k, v in adjlists.items():
            for j in v:
                edges.append((k, j))
        graph.add_nodes_from(nodes)
        graph.add_edges_from(edges)
        return graph

    # XXX equivalent to adjlist
    def edgelist(self, low, high, edge):
        adjlists = {}
        while low < high:
            partial = self._graph(low, low + self._maxquery, edge)
            adjlists.update(partial)
            low += self._maxquery
        edges = []
        for k, v in adjlists.items():
            for j in v:
                edges.append((k, j))
        return torch.tensor(edges)

    def missing_values(self, predicate, low, high):
        return []
        query = (
                '{missing_values (func: ge(%s, %d), orderasc: %s) '
                '@filter(lt(%s, %d) AND (NOT has(%s))) {%s}}' %
                    (self.sorted_predicate, low, self.sorted_predicate,
                     self.sorted_predicate, high, predicate, self.id_predicate
                    )
                )
        res = self.query(query)
        d = json.loads(res.json.decode())
        return [i[self.id_predicate] for i in d['missing_values'] ]

    def max(self, predicate=None):
        if predicate is None:
            predicate = self.sorted_predicate
        query = ('{'
                  'var(func: has(%s)) {'
                    'v as %s'
                  '}'
                  'max() {'
                    'max(val(v))'
                  '}'
                '}' % (predicate, predicate)
                )
        res = self.query(query)
        d = json.loads(res.json.decode())
        return d['max'][0]['max(val(v))']

#    def count(self, predicate=None):
#        if predicate is None:
#            predicate = self.id_predicate
#        query = ('{'
#                  'var(func: has(%s)) {'
#                    'v as count(%s)'
#                  '}'
#                  'sum() {'
#                    'sum(val(v))'
#                  '}'
#                '}' % (predicate, predicate)
#                )
#        res = self.query(query)
#        d = json.loads(res.json.decode())
#        return d['sum'][0]['sum(val(v))']

    def one(self, predicate, identity=None):
        if identity:
            if isinstance(identity, str):
                query = '{ one(func: eq(%s, "%s")) { %s }}' % (
                    self.id_predicate, identity, predicate)
            else:
                query = '{ one(func: eq(%s, %s)) { %s }}' % (
                    self.sorted_predicate, identity, predicate)
        else:
            query = (
                    '{ one(func: has(%s), first: 1) { %s }}'
                    %(predicate, predicate)
                    )
        res = self.query(query)
        d = json.loads(res.json.decode())
        r = d['one'][0][predicate]
        t = self.deserialize_type(predicate)
        if t:
            r = self.deserialize([r], t)[0]
        return r

    @staticmethod
    def _idx2str(identities, low, high):
        if isinstance(identities[0], str):
            if isinstance(identities, np.ndarray):
                tmp = identities[low:high].tolist()
            else:
                tmp = identities[low:high]
            nodes = json.dumps(tmp)
        elif isinstance(identities, np.ndarray):
            nodes = ', '.join([str(s) for s in identities[low:high]])
        else:
            nodes = identities[low:high]

        return nodes

    # return values are NOT same order as identities
    # predicates: returned predicates
    # identities_predicate: predicate used for identities argument
    def batch(self, identities, predicates, identities_predicate=None,
              identities_sorted=False):

        if identities_predicate is None:
            identities_predicate = self.id_predicate

        fltr = '@filter('
        for i, p in enumerate(self.all_predicates()):
            if i > 0:
                fltr += ' AND '
            fltr += 'has(%s)'%p
        fltr += ') '

        if identities_sorted:
            sort = ', orderasc: %s '%identities_predicate
        else:
            sort = ''

        # split query
        step = 250 # for PyG 1000 doesn't work
        l = 0
        h = step
        lim = len(identities)
        ret = OrderedDict()


        while l < lim:
            nodes = dgraph._idx2str(identities, l, h)
            query = (
                    '{ batch(func: eq(%s, %s)%s) %s { %s }}' %
                    (identities_predicate, nodes, sort, fltr,
                     ' '.join(predicates))
                    )
            res = self.query(query)
            d = json.loads(res.json.decode())
            for p in predicates:
                t = self.deserialize_type(p)
                if t:
                    tmp = [self.deserialize([i[p]], t)[0] for i in d['batch']]
                else:
                    tmp = [i[p] for i in d['batch']]
                if p in ret:
                    ret[p].extend(tmp)
                else:
                    ret[p] = tmp
            l = h
            h = h + step
        
        return ret


    @staticmethod
    def _nested_pred(edge, pred, id_pred, l):
        if pred in edge:
            for e in edge[pred]:
                dgraph._nested_pred(e, pred, id_pred, l)
            return
        l.append(edge[id_pred])

    def neighbors(self, identities, hop=1, local=False,
                  pred=None, id_pred=None):
        r"""return a dictionary whose keys indicate sources and values 
        indicate destinations within 'identities'. Invalid edges are
        filtered out.
        """

        pred = self.edge_attribute if pred is None else pred
        id_pred = self.id_predicate if id_pred is None else id_pred

        if local:
            allb = dgraph._idx2str(identities, 0, len(identities))
            fltr = '@filter(eq(%s, %s))'%(id_pred, allb)
        else:
            fltr = ''

        step = 128
        l = 0
        h = step
        lim = len(identities)

        ret = {}
        while l < lim:
            b = dgraph._idx2str(identities, l, h)

            if hop == 1:
                query = (
                        '{ neighbors(func: eq(%s, %s)) @filter(has(%s)) '
                        '@cascade { %s %s {%s} %s} }'
                         % (id_pred, b, pred, id_pred, pred, id_pred, fltr)
                        )
            else:
                query = (
                        '{ neighbors(func: eq(%s, %s)) @filter(has(%s)) '
                        '@cascade { %s '% (id_pred, b, pred, id_pred)
                        )
                for i in range(hop):
                    query += '%s { '%pred
                query += '{ %s }%s'%(id_pred, fltr)
                for i in range(hop):
                    query += ' }'
                query += ' } }'

            res = self.query(query)
            d = json.loads(res.json.decode())
            if hop == 0:
                # d['neighborss'] generates a list of given identities,
                # and n[pred] generates a list of neighbors
                ret.update({n[id_pred]: set([i[id_pred] for i in n[pred]])
                        for n in d['neighbors']})
            else:
                for e in d['neighbors']:
                    tmp = []
                    dgraph._nested_pred(e, pred, id_pred, tmp)
                    tmp = set(tmp)
                    me = e[id_pred]
                    if me in tmp:
                        tmp.remove(me)
                    ret.update({me: tmp})
            l = h
            h = h + step
        return ret

    def noisolate(self, nodes):
        edge = self.edge_attribute
        id_pred = self.id_predicate

        step = 128
        l = 0
        h = step
        lim = len(nodes)
        cnt = 0

        while l < lim:
            b = nodes[l:h]
            if isinstance(nodes, list):
                if isinstance(nodes[0], str):
                    b = json.dumps(b)
            elif isinstance(nodes, np.ndarray):
                if isinstance(nodes[0], str):
                    b = json.dumps(b.tolist())
            query = ('{'
                      'noisolate (func: eq(%s, %s)) @filter(has(%s)) '
                       '{ count(uid) } '
                     '}'%(id_pred, b, edge)
                    )
            res = self.query(query)
            d = json.loads(res.json.decode())
            cnt += d['noisolate'][0]['count']
            l = h
            h += step
        return cnt == len(nodes)

    def load_dataset(self, dataset, predicates, n=0):
        # based on read_planetoid_data() of pyg
        nnodes, nfeatures = dataset[0].x.shape
        while n < nnodes:
            prev_n = n
            nquads, n, names = self.nquads_dataset(dataset, predicates, n)
            self.mutate(nquads, lids=[i for i in range(prev_n, n)], names=names)

        with open('uidmap.pkl', 'wb') as f:
            pickle.dump(self.uidmap, f)
        print('done uidmap')

    def load_df(self, df, predicates, n=0):

        print('loading df')
        while n < len(df):
            prev_n = n
            nquads, n, names = self.nquads(df, predicates, n)
            self.mutate(nquads, lids=[x for x in range(prev_n, n)], names=names)
            # Dump uidmap, as we may fail later
        print('dumping uidmap')
        with open('uidmap.pkl', 'wb') as f:
            pickle.dump(self.uidmap, f)
        print('done uidmap')

    def load_graph(self, g, edge):
        nodes = list(g.nodes()) # XXX
        print('loading edges')
        n = 0
        if len(self.uidmap) == 0:
            with open('uidmap.pkl', 'rb') as f:
                d = pickle.load(f)
                self.uidmap.update(d)
        j = 0
        nbrs = None
        num_nodes = len(nodes)
        cnt = 0
        n_prev = 0
        while n < num_nodes:
            nquads, n, nbrs, j, c = self.nquads_edges(g, edge, n,
                    nodes=nodes, neighbors=nbrs, j=j)
            self.mutate(nquads)
            cnt += c
            if n > n_prev + 10000:
                print('%d / %d'%(n, num_nodes))
                n_prev = n
        print('%d edges committed'%cnt)

    def nquads_edge_index(self, edge_index, label, i):
        nquads = ''
        budget = self._maxquery

        for edge in edge_index.t()[i:]:
            edge = edge.tolist()
            uids = []
            for node in edge:
                try:
                    uids.append(self.uidmap[node])
                except:
                    print('%s is not in uidmap'%node)
            if len(uids) < 2:
                raise Exception
            nquads += '<%s> <%s> <%s> .\n' % (uids[0], label, uids[1])
            budget -= 1
            if budget == 0:
                break
        count = self._maxquery - budget
        return nquads, i + count, count

    def load_graph_dataset(self, dataset, label):

        i = 0
        cnt = 0
        edge_index = dataset[0].edge_index
        n_prev = 0
        while cnt < edge_index.shape[1]:
            nquads, i, c = self.nquads_edge_index(edge_index, label, i)
            self.mutate(nquads)
            cnt += c
            if cnt > n_prev + 1000:
                print('{} / {}'.format(i, edge_index.shape[1]))
                n_prev = cnt

if __name__ == '__main__':
    pass

