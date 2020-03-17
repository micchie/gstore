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
from abc import ABCMeta, abstractmethod
from scipy.sparse import csr_matrix
import scipy
import base64
import io
import numpy as np
import pandas as pd
from collections import OrderedDict
import random
import torch
import functools
from torch_geometric.data import InMemoryDataset, Data

#def connect_dgraph():
#    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#    client.connect(("localhost", 5080))
#    client.send(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
#    response = client.recv(4096)
#    print("response", response)
#

class absgraph(metaclass=ABCMeta):
    __metaclass__ = ABCMeta
    def __init__(self, schema, transform=None):
        self.schema = schema
        self.id_predicate, self.sorted_predicate, self.edge_attribute = \
                self.predicate_roles(schema)

        self.id_predicate_unsortable = False
        if schema[self.id_predicate]['type'] != 'int':
            self.id_predicate_unsortable = True
        if not self.sorted_predicate:
            self.sorted_predicate = self.id_predicate
        random.seed()
        if transform:
            self._transform = transform
        else:
            self._transform = None

    def predicate_roles(self, d):
        id_predicate = sorted_predicate = edge_attribute = None
        for k, v in d.items():
            if '_id' in v:
                id_predicate = k
            if '_sorted' in v:
                sorted_predicate = k
            if 'edge' in v:
                edge_attribute = k
        return id_predicate, sorted_predicate, edge_attribute

    def all_predicates(self, incl_edge=False):
        return [k for k, v in self.schema.items() if not 'edge' in v]

    @abstractmethod
    def drop_all(self):
        pass

    @abstractmethod
    def get_schema(self):
        pass

    @abstractmethod
    def load_schema(self):
        pass

    @abstractmethod
    def set_index(self):
        pass

    @abstractmethod
    def count(self, name=None):
        pass

    @abstractmethod
    def nquads(self, df, predicates, i):
        pass

    @abstractmethod
    def nquads_edges(self, graph, label, i=0, nodes=None, neighbors=None, j=0):
        pass

    @abstractmethod
    def neighbors(self, identities, hop=1, local=False,
                  pred=None, id_pred=None):
        pass

    @abstractmethod
    def batch(self, identities, predicates, decode_predicates=[],
              identities_predicate=None):
        pass

    @abstractmethod
    def missing_values(self, predicate, low, high):
        pass

    @abstractmethod
    def one(self, predicate, identity=None):
        pass

    @abstractmethod
    def load_df(self, df, predicates, n=0):
        pass

    @abstractmethod
    def load_graph(self, g, edge):
        pass

    @abstractmethod
    def _range(self, low, high, predicates, id_predicate=None, expand=False):
        pass

    def random(self, predicate, low, high, n, required_predicates=[]):
        res = []

        while len(res) < n:
            nums = random.sample(range(low, high + 1), n)
            # reordering does not patter
            r = self.batch(nums, [predicate],
                    identities_predicate=self.sorted_predicate)[predicate]
            res.extend(r)
        return res[:n]

    def batch_ordered(self, identities, predicates,
                      identities_predicate=None,
                      identities_sorted=False):

        ret = self.batch(identities, predicates,
                         identities_predicate=identities_predicate,
                         identities_sorted=identities_sorted)
        if identities_sorted:
            return ret

        od = OrderedDict()
        for p in predicates:
            od[p] = []
            for i in identities:
                idx = -1
                for j, v in enumerate(ret[self.id_predicate]):
                    if v == i:
                        idx = j
                        break
                if idx != -1:
                    od[p].append(ret[p][idx])
                else:
                    print('no entry for ', p, i)
        return od

    def batch_tensor(self, identities, predicates, identities_sorted):
        if not self.id_predicate in predicates:
            predicates.insert(0, self.id_predicate)
        r = self.batch_ordered(identities, predicates,
                               identities_sorted=identities_sorted)
        ret = {}
        for p in [x for x in predicates if x != self.id_predicate]:
            if isinstance(r[p][0], torch.Tensor):
                ret[p] = torch.stack(r[p])
            else:
                ret[p] = r[p]
        return ret

    @staticmethod
    def _val2idx(identities, v, cache):
        if v in cache:
            return cache[v]
        i = identities.index(v)
        cache[v] = i
        return i

    def batch_edge_index(self, identities, squeeze=True):
        r"""Returns an edge_list of pytorch-geometric based on the given
        identities. 'squeeze' option squeezes edge indices to start from
        zero and preserve the order. This helps the caller compose
        'torch_geometric.Data' whose 'x' uses the row number as node index.
        """
        r = self.neighbors(identities, local=True)
        l = []
        cache = {}
        for src, dsts in r.items():
            for dst in dsts:
                if squeeze:
                    s = absgraph._val2idx(identities, src, cache)
                    d = absgraph._val2idx(identities, dst, cache)
                else:
                    s = src
                    d = dst
                l.append(torch.tensor([s, d]))
        return torch.stack(l).t()

    def batch_data(self, identities, predicates, identities_sorted=False):
        r"""Returns torch_geometric.data.Data that applies self.transform.
        See "Creating Your Own Datasets" section in PyG document.
        XXX We should support better integration with existing datasets
        """
        ret = self.batch_tensor(identities, predicates, identities_sorted)
        edge_index = self.batch_edge_index(identities, squeeze=True)
        data = Data(x=ret['x'], y=ret['y'], edge_index=edge_index)
        data = data if self._transform is None else self._transform(data)
        return data

    def _dataframe(self, low, high, predicates):
        r = self._range(low, high, predicates)
        df_src = []
        for p in predicates:
            x = [ i[p] for i in r]
            if p in self.schema:
                t = self.deserialize_type(p)
                if t:
                    x = self.deserialize(x, t)
            df_src.append(x)
        df = pd.DataFrame(df_src).T
        df.columns = predicates
        return df

    def dataframe(self, low, high, predicates):
        dfs = []
        #high = min(self.count(), high)
        while low < high:
            df = self._dataframe(low, min(low + self._maxquery, high),
                    predicates)
            dfs.append(df)
            low += self._maxquery
        return pd.concat(dfs, ignore_index=True)

    def deserialize_type(self, predicate):
        if '_type' in self.schema[predicate]:
            return self.schema[predicate]['_type']
        return None

    def deserialize(self, serialized, how):
        deserialized = []
        for s in serialized:
            sd = base64.b64decode(s)
            f = io.BytesIO()
            f.write(sd)
            f.seek(0)
            if how == 'ndarray':
                d = np.load(f)
            elif how == 'csr_matrix':
                d = scipy.io.mmread(f)
                d = d.tocsr()
            elif how == 'tensor':
                d = torch.load(f)
            else:
                raise Exception
            f.close()
            deserialized.append(d)
        return deserialized

    def serialize(self, obj):
        if isinstance(obj, csr_matrix):
            wf = scipy.io.mmwrite
        elif isinstance(obj, np.ndarray):
            wf = np.save
        elif isinstance(obj, torch.Tensor):
            wf = lambda x, y: torch.save(y, x)
        else:
            return obj
        f = io.BytesIO()
        wf(f, obj)
        f.seek(0)
        obj = base64.b64encode(f.read()).decode('utf-8')
        f.close()
        return obj

if __name__ == '__main__':
    v = absgraph()
