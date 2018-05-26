import hdfs
from urllib.parse import urlparse

class hdfs:

    def __int__(self, domain):
        self.client = hdfs.client.Client("http://" + urlparse(domain).netloc)

    def read(self, path):
        return self.client.read(urlparse(path).path)