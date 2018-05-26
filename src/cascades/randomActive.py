import random
import networkx as nx

from .abstract_cascade import abstract_cascade

class randomActive(abstract_cascade):
    def next(self):
        if self.step < self.iterations and len(self.n) > 0:
            if self.decision(0.85):
                activate = random.choice(self.G.nodes())
                self.activated = activate
                if activate != None:
                    nx.set_node_attributes(self.G, 'activated', {activate: self.G.node[activate]['activated'] + 1})
                    if self.G.node[activate]['activated'] == 1:
                        self.n.discard(activate)
                        self.a.add(activate)

            else:
                self.cascase_id += 1
                seed = random.sample(self.n, 1)[0]
                self.n.discard(seed)
                self.a.add(seed)
                self.activated = seed
                nx.set_node_attributes(self.G, 'activated', {seed: 1})

            self.step += 1
        else:
            raise StopIteration()
