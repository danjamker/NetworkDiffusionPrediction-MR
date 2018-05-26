from copy import deepcopy
import random
from abc import abstractmethod

class abstract_cascade:
    def __init__(self, G, itterations=10000):

        self.G = deepcopy(G)
        self.cascase_id = 1
        self.step = 1
        self.d = {}
        self.activated = ""
        self.numberOfNodes = len(self.G.nodes())
        self.numberactivated = 0
        self.n = set([n for n, attrdict in self.G.node.items() if attrdict['activated'] == 0])
        self.a = set([n for n, attrdict in self.G.node.items() if attrdict['activated'] > 0])
        self.iterations = itterations
        self.step_time = None
        self.tag = "a"

    def __iter__(self):
        return self

    def decision(self, probability):
        '''
        Returns a True/False dissision bases on a random distribution and a probability threshold.
        :param probability: Probability threshold
        :type probability: int
        :return: True/False
        :rtype: bool
        '''
        return random.random() < probability

    @abstractmethod
    def next(self):
        pass

    def getInfectedNode(self):
        """

        :return:
        :rtype:
        """
        return self.activated

    def getDeepGraph(self):
        return deepcopy(self.G)

    def getGraph(self):
        return self.G

    def getStep(self):
        '''
        Returns the current iteration the cascade is in
        :return: step number
        :rtype: int
        '''
        return self.step

    def getStepTime(self):
        return self.step_time

    def getTag(self):
        return self.tag

