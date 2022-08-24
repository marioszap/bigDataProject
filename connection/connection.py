from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config= {
        'secure_connect_bundle': 'secure-connect-bigdataproject.zip'
}
auth_provider = PlainTextAuthProvider("FKDIkqtNHPhiyxCZFNdzeKMS", "rAleUiXfRycLJkw,c,7c30MDBSp3aqeTZlIuWQlQDGTyNvg---GnNFh9bOqfvYWpdoWZNmtWU3SDlffApXTawewwOCgvAICn-xyTkMb7pHKY.HtN7tLfLBDhDw+57nf6")
cluster = Cluster(cloud = cloud_config, auth_provider = auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
    print(row[0])
else:
    print("An error occurred.")
