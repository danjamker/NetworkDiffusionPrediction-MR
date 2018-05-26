import random
import networkx as nx
import pandas as pd

from .abstract_cascade import abstract_cascade

class actualCascade(abstract_cascade):
    def __init__(self, file, G):
        self.G = G
        self.f = file
        self.cascase_id = 1
        self.step = 1
        self.d = {}
        self.activated = ""

        dtf = pd.read_csv(file, index_col=False, header=None, sep="\t", engine="python",
                          compression=None, names=["word", "tag", "node", "time"]).drop_duplicates(subset=["time"],
                                                                                            keep='last')
        dtf['time'] = pd.to_datetime(dtf['time'], dayfirst=True)
        # Filters out users that are not in the network
        dftt = dtf[dtf["node"].isin(self.G.nodes())]
        self.df = dftt.set_index(pd.DatetimeIndex(dftt["time"])).sort_index();
        self.dfi = self.df.iterrows();

        # self.name_to_id = dict((d["name"], n) for n, d in self.G.nodes_iter(data=True))
        self.name_to_id = dict((n, n) for n, d in self.G.nodes_iter(data=True))
        self.tag = "a"

    def next(self):
        try:
            activate = next(self.dfi)
            self.activated_name = activate[1]["node"]
            self.step_time = activate[1]["time"]
            self.activated = self.name_to_id[self.activated_name]
            self.tag = activate[1]["tag"]
            if self.G.has_node(self.name_to_id[self.activated]):
                nx.set_node_attributes(self.G, 'activated',
                                       {self.activated: self.G.node[self.activated]['activated'] + 1})
                nx.set_node_attributes(self.G, 'time',
                                       {self.activated: self.step_time})
            else:
                self.activated = None

            self.step += 1
            self.step_time = activate[1]["time"]

        except EOFError:
            raise StopIteration()
        except IndexError:
            raise StopIteration()
        except StopIteration:
            raise StopIteration()
        except KeyError:
            pass
