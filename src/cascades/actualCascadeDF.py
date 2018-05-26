import random
import networkx as nx

from .abstract_cascade import abstract_cascade

class actualCascadeDF(abstract_cascade):
    def __init__(self, JSON, G):
        # self.G = deepcopy(G)
        self.G = G
        self.f = file
        self.cascase_id = 1
        self.step = 1
        self.d = {}
        self.activated = ""
        dtf = pd.read_json(JSON)
        self.df = dtf.sort_index();
        self.dfi = self.df.iterrows();
        self.step_time = None

        # self.name_to_id = dict((d["name"], n) for n, d in self.G.nodes_iter(data=True))
        self.name_to_id = dict((n, n) for n, d in self.G.nodes_iter(data=True))

    def next(self):
        try:
            activate = next(self.dfi)
            self.activated_name = activate[1]['id']
            self.activated = self.name_to_id[self.activated_name]
            if self.G.has_node(self.name_to_id[self.activated]):
                nx.set_node_attributes(self.G, 'activated',
                                       {self.activated: self.G.node[self.activated]['activated'] + 1})
            else:
                self.activated = None

            self.step += 1

        except EOFError:
            raise StopIteration()

        except IndexError:
            raise StopIteration()

        except StopIteration:
            raise StopIteration()
        except KeyError:
            pass
