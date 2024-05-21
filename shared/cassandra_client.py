from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts, load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),protocol_version=4)
        self.session = self.cluster.connect()

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query, parameters=None):
        if parameters is not None:
            return self.get_session().execute(query, parameters)
        else:
            return self.get_session().execute(query)
