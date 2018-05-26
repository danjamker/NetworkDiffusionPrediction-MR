import random
import networkx as nx

from .abstract_cascade import abstract_cascade

class NodeInSameCommunity(abstract_cascade):
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
                nx.set_node_attributes(self.G, 'activated', {seed: 1})
                self.n.discard(seed)
                self.a.add(seed)

            self.step += 1
        else:
            raise StopIteration()

    def select(self, G, node):
        c = [n for n in G.neighbors(node) if G.node[n]['community'] == G.node[node]["community"]]
        if len(c) > 0:
            return random.choice(c)
        else:
            return None
