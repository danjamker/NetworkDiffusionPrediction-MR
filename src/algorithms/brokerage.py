class _RoleClassifier(object):
    roleTypes = {
        "coordinator"	: lambda pred ,broker ,succ: pred == broker == succ,
        "gatekeeper" 	 	: lambda pred ,broker ,succ: pred != broker == succ,
        "representative"	: lambda pred ,broker ,succ: pred == broker != succ,
        "consultant"		: lambda pred ,broker ,succ: pred == succ != broker,
        "liaison"			: lambda pred ,broker ,succ: pred != succ and pred != broker and broker != succ,
        }

    @classmethod
    def classify(cls ,predecessor_group ,broker_group ,successor_group):
        for role ,predicate in cls.roleTypes.iteritems():
            if predicate(predecessor_group ,broker_group ,successor_group):
                return role
        raise Exception("Could not classify... this should never happen")

def getBrokerageRoles(graph ,partition):

    roleClassifier = _RoleClassifier()

    roles = dict((node, dict((role ,0) for role in roleClassifier.roleTypes)) for node in graph)
    for node in graph:
        for successor in graph.successors(node):
            for predecessor in graph.predecessors(node):
                if successor == predecessor or successor == node or predecessor == node: continue
                if not (graph.has_edge(predecessor, successor)):
                    roles[node][roleClassifier.classify(graph.node[predecessor][partition] ,graph.node[node][partition] ,graph.node[successor][partition])] += 1
    return roles