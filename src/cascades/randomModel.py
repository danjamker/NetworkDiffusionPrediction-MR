import random
import networkx as nx

from .abstract_cascade import abstract_cascade

class randomModel(abstract_cascade):
    def next(self):
        if self.step < self.iterations and len(self.n) > 0:
            activate = random.choice([n for n, attrdict in self.G.node.items() if attrdict['activated'] == 0])
            self.activated = activate
            if activate != None:
                nx.set_node_attributes(self.G, 'activated', {activate: self.G.node[activate]['activated'] + 1})
            self.d[self.step] = len([n for n, attrdict in self.G.node.items() if attrdict['activated'] > 0]) / len(
                self.G.nodes())
            self.step += 1
        else:
            raise StopIteration()


