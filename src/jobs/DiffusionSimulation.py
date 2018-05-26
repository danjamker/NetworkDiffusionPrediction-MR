from __future__ import division

import gzip

import io
from urllib.parse import urlparse

import networkx as nx
import pandas as pd
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.step import MRStep

from src.util.dataloader import hdfs, s3, local
from src import cascades

import random
class MRJobNetworkXSimulations(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def configure_options(self):
        super(MRJobNetworkXSimulations, self).configure_options()
        self.add_file_option('--network')
        self.add_passthru_arg('--modle', type=int, default=0, help='...')
        self.add_passthru_arg('--sampelFraction', type=int, default=10, help='...')
        self.add_passthru_arg('--resampeling', type=int, default=10, help='...')
        self.add_passthru_arg('--iteration_miltiplyer', type=int, default=10, help='...')
        self.add_passthru_arg('--numberofloops', type=int, default=100, help='...')
        self.add_passthru_arg('--filesystem', type=str, default='local', help='Which files system will this be reading from')

    def runCascade(self, C):
        cas = C
        idx = []
        values = []
        while True:
            try:
                cas.next()
                values.append(cas.getInfectedNode())
                idx.append(cas.getStep())
            except StopIteration as err:
                break
            except Exception as e:
                print(e)
        return idx, values

    def csize_init(self):
        self.G = nx.read_gpickle(self.options.network)
        print(self.G.nodes(data=True))

    def csize(self, _, line):

        if self.options.filesystem == "hdfs":
            client = hdfs.hdfs("http://" + urlparse(line).netloc)
        elif self.options.filesystem == "s3":
            client = s3.s3("http://" + urlparse(line).netloc)
        else:
            client = local.local()

        if line[-1] != "#":
            with client.read(urlparse(line).path) as r:
                # with open(urlparse(line).path) as r:
                buf = io.BytesIO(r.read())

                # If the data is in a GZipped file.
                if ".gz" in line:
                    gzip_f = gzip.GzipFile(fileobj=buf)
                    content = gzip_f.read()
                    buf = io.StringIO(content)

                #Read in the CSV file
                dftt = self.read_csv(buf)
                print(dftt)
                if len(dftt.index) > 0:
                    yield len(dftt.index), _

    def read_csv(self, buffer):
        dtf = pd.read_csv(buffer, index_col=False, sep=",", engine="python", compression=None)
        dtf = dtf.drop_duplicates(keep='last')
        return dtf[dtf["Node"].isin(self.G.nodes())]

    def csize_red(self, length, _):
        yield _, length

    def mapper_init(self):

        self.G = nx.read_gpickle(self.options.network)
        self.default_node_values = {node: 0 for node in self.G.nodes()}
        nx.set_node_attributes(self.G, 'activated', self.tmp)

        nx.set_node_attributes(self.G, 'activated', {node: 0 for node in self.G.nodes()})
        seed = random.choice([n for n, attrdict in self.G.node.items() if attrdict['activated'] == 0])
        nx.set_node_attributes(self.G, 'activated', {seed: 1})

        self.r_u_l = None
        self.r_a_l = None

    def mapper(self, _, diffusion_size):

        iteration = int(diffusion_size) * self.options.iteration_miltiplyer
        for x in range(0, self.options.numberofloops):

            nx.set_node_attributes(self.G, 'activated', self.default_node_values)
            seed = random.choice([n for n, attrdict in self.G.node.items() if attrdict['activated'] == 0])
            nx.set_node_attributes(self.G, 'activated', {seed: 1})

            if self.options.modle == 0:
                idx, values = self.runCascade(cascades.randomActive(self.G, itterations=iteration))
            elif self.options.modle == 1:
                idx, values = self.runCascade(cascades.CascadeNabours(self.G, itterations=iteration))
            elif self.options.modle == 2:
                idx, values = self.runCascade(cascades.NodeWithHighestActiveNabours(self.G, itterations=iteration))
            elif self.options.modle == 3:
                idx, values = self.runCascade(cascades.NodeInSameCommunity(self.G, itterations=iteration))
            elif self.options.modle == 4:
                idx, values = self.runCascade(cascades.CascadeNaboursWeight(self.G, itterations=iteration))

            df = pd.DataFrame({"ids": values}, index=idx)

            for i in range(1, self.options.resampeling):
                yield _, df.sample(frac=(float(self.options.sampelFraction) / float(100))).to_json()

    def steps(self):
        #First stage of this gets the size of each diffusion
        #The second stage the simulates diffusions with diffrent models.
        return [
            MRStep(
                mapper_init=self.csize_init,
                mapper=self.csize,
                reducer=self.csize_red
            ),
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper
            )
        ]


if __name__ == '__main__':
    MRJobNetworkXSimulations.run()
