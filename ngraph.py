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
import pandas as pd
import numpy as np
from collections import OrderedDict
from neo4j import GraphDatabase
from rgraph import rgraph
import os
import time
import copy

# default port is 7687
class ngraph(rgraph):
    def __init__(self, server, schema, transform=None, noconnect=False):
        server = 'bolt://' + server
        if not noconnect:
            self._driver = GraphDatabase.driver(server, auth=('neo4j', "root"))
        self._maxquery = 100

        super(rgraph, self).__init__(schema) # XXX

    def drop_all(self):
        self.query('MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r')

    def get_schema(self):
        pass

    def load_schema(self, schema):
        pass

    def query(self, query):
        with self._driver.session() as s:
            r = s.write_transaction(lambda tx: tx.run(query))
        return r

    def _nquads(self, nquads):
        l = []
        for i in nquads:
            e = ''
            for k, v in i.items():
                if len(e) > 0:
                    e += ', '
                if isinstance(v, int) or isinstance(v, np.int64):
                    e += '%s: %d'%(k, v)
                else:
                    e += '%s: "%s"'%(k, v)
            e = '(: node {' + e + '})'
            l.append(e)
        return 'CREATE ' + ', '.join(l)

    def load_df(self, df, predicates, n=0):
        while n < len(df):
            nquads, n = self.nquads(df, predicates, n)
            nquads = self._nquads(nquads)
            self.query(nquads)

    def parse_count(self, res):
        return [r['COUNT(n)'] for r in res][0]

    def _range(self, low, high, predicates, id_predicate=None, expand=False):
        query = self.range_cypher(low, high, predicates, id_predicate, expand)
        ret = OrderedDict({p:[] for p in predicates})
        res = self.query(query)

        for r in res:
            for p in predicates:
                ret[p].append(r['n.%s'%p])
        return self._range_xform(ret, predicates)

    def parse_one(self, res, predicate):
        return [r['n.%s'%predicate] for r in res][0]

    def parse_batch(self, res, ret, predicates):
        for r in res:
            break
        else:
            return

        tmp = OrderedDict({p: [] for p in predicates})
        for k, v in r.items():
            p = k.split('.')[1]  # numeric, name etc
            tmp[p].append(v)
        for p in predicates:
            l = tmp[p]
            t = self.deserialize_type(p)
            if t:
                l = self.deserialize(l, t)
            ret[p].extend(l)

    def parse_neighbors(self, res, ret):
        for s, n in res:
            if s in ret:
                ret[s].append(n)
            else:
                ret[s] = [n]

    # Unlike RedisGraph, WHERE based match is faster in Neo4j
    def batch(self, identities, predicates, identities_predicate=None):

        if identities_predicate is None:
            identities_predicate = self.id_predicate

        if isinstance(identities[0], str):
            where_base = 'n.%s = "%s"'
        else:
            where_base = 'n.%s = %s'

        step = 1000
        l = 0
        h = step
        lim = len(identities)
        ret = OrderedDict({p: [] for p in predicates})
        while l < lim:
            nodes = identities[l:h]
            where = [where_base%(identities_predicate, i) for i in nodes]
            where = ' OR '.join(where)
            rtrn = ['n.%s'%p for p in predicates]
            #if not identities_predicate in predicates:
            #    rtrn.append('n.' + identities_predicate)
            rtrn = ', '.join(rtrn)
            query = 'MATCH (n: node) WHERE %s RETURN %s'%(where, rtrn)
            res = self.query(query)

            tmp = OrderedDict({p: [] for p in predicates})
            for i, r in enumerate(res):
                for p in predicates:
                    tmp[p].append(r['n.' + p])
            for p in predicates:
                l = tmp[p]
                t = self.deserialize_type(p)
                if t:
                    l = self.deserialize(l, t)
                ret[p].extend(l)
            l = h
            h = h + step
        return ret

    def nquads_csv(self, df, g, predicates, edge, i, lim, path):

        f = open(os.path.join(path, 'nodes.csv'), 'w')
        # Header
        header = []
        for p in predicates:
            s = p
            if p == self.id_predicate:
                s += ':ID'
            if self.schema[p]['type'] == 'int':
                s += ':int'
            header.append(s)
        header.append(':LABEL')
        header = ','.join(header) + '\n'
        f.write(header)

        while i < lim:
            row = []
            for p in predicates:
                try:
                    s = df.iloc[i][p]
                except:
                    s = str(i)
                if p == self.id_predicate:
                    s = '"' + s + '"'
                else:
                    s = self.serialize(s)
                row.append(s)
            row.append('node')
            row = ','.join(row) + '\n'
            f.write(row)
            i += 1
        f.close()

        f = open(os.path.join(path, 'edges.csv'), 'w')
        header = [':START_ID', edge, ':END_ID', ':TYPE']
        header = ','.join(header) + '\n'
        f.write(header)

        i = 0
        while i < lim:
            me = df[predicates[0]][i]
            for nei in g.neighbors(me):
                row = [me, edge, nei, edge]
                row = ','.join(row) + '\n'
                f.write(row)
            i += 1
        f.close()

import sys
if __name__ == '__main__':
    server = 'localhost:7687'
    predicates = ['name', 'numeric', 'identity']
    edge = 'edge'
    schema = {predicates[0]: {'type': 'string', 'index': 'hash', '_id': True},
              predicates[1]: {'type': 'string', '_type': 'ndarray'},
              predicates[2]: {'type': 'int', 'index': 'int', '_sorted': True},
                       edge: {'type': 'string', 'edge': True}
             }
    ng = ngraph(server, schema)
    r = ng.batch([i for i in range(1000)], ['name'], identities_predicate='identity')
    #print(r)

    sys.exit()

    # Delete everything
    ng.drop_all()
    # Add node
    ng.query('CREATE (:node {name: "acc-tight5.mps.pkl__v998"})')
    # Lookup the node
    r = ng.query('MATCH (n:node {name: "acc-tight5.mps.pkl__v998"}) RETURN n.name')
    for i in r:
        print(i['n.name'])
    # Add another node with name and integer attribute
    ng.query('CREATE (:node {name: "acc-tight5.mps.pkl__v10", identity: 2})')
    r = ng.query('MATCH (n:node {identity: 2}) RETURN n.name, n.identity')
    for i in r:
        print(i['n.name'], i['n.identity'])

    ng.drop_all()
