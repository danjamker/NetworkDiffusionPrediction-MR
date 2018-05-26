import random
import networkx as nx

from .abstract_cascade import abstract_cascade

class NodeWithHighestActiveNabours(abstract_cascade):
    def next(self):
        if self.step < self.iterations and len(self.n) > 0:
            if self.decision(0.85):
                node = random.sample(self.a, 1)[0]
                activate = self.select(self.G, node)
                self.activated = activate
                if activate != None:
                    self.n.discard(activate)
                    self.a.add(activate)
                    nx.set_node_attributes(self.G, 'activated', {activate: self.G.node[activate]['activated'] + 1})
            else:
                self.cascase_id += 1
                seed = random.sample(self.n, 1)[0]
                self.activated = seed
                self.n.discard(seed)
                self.a.add(seed)
                nx.set_node_attributes(self.G, 'activated', {seed: 1})

            self.step += 1
        else:
            raise StopIteration()

    def select(self, G, node):
        t = None
        tc = 0
        for s in G.neighbors(node):
            tmp = G.neighbors(s)
            c = len([n for n in tmp if G.node[n]['activated'] > 0]) / len(tmp)
            if t == None:
                if c > 0:
                    t = s
                    tc = c
            elif c > tc:
                t = s
                tc = c
        return t

