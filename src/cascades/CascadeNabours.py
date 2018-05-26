import random
import networkx as nx

from .abstract_cascade import abstract_cascade

class CascadeNabours(abstract_cascade):
    def next(self):
        if self.step < self.iterations and len(self.n) > 0:
            if self.decision(0.85):
                node = random.sample(self.a, 1)[0]
                l = self.G.neighbors(node)
                if len(l) > 0:
                    activate = random.choice(l)
                else:
                    activate = None

                self.activated = activate
                if activate != None:
                    nx.set_node_attributes(self.G, 'activated', {activate: self.G.node[activate]['activated'] + 1})
                    self.n.discard(activate)
                    self.a.add(activate)
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
