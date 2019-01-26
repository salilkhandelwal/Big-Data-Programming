import os, gzip, sys
from cassandra.cluster import Cluster
import uuid
import re, datetime
from cassandra.query import BatchStatement

assert sys.version_info >= (3, 4)

cluster = Cluster(['199.60.17.188', '199.60.17.216'])
reg = "^(\\S+) - - \\[(\\S+) [+-]\\d+\\] \"[A-Z]+ (\\S+) HTTP/\\d\\.\\d\" \\d+ (\\d+)$"
linere = re.compile(reg)

def parseline(line):
    match = re.search(linere, line)
    if match:
        m = re.match(linere, line)
        host = m.group(1)
        date = datetime.datetime.strptime(m.group(2), '%d/%b/%Y:%H:%M:%S')
        path = m.group(3)
        bys = float(m.group(4))
        return (host, date, path, bys)
    return None

def main(inputs,keyspace,table):
    session = cluster.connect(keyspace)
    insert_user = session.prepare("INSERT INTO %s (uid, host, datetime, path, bytes)\
        VALUES (?, ?, ? , ?, ?)" % table )
    BATCH_SIZE = 300
    batch = BatchStatement()

    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', \
            encoding='utf-8', errors='ignore') as logfile:

            count = 0

            for line in logfile:
                l = parseline(line)
                if l is not None:
                    (host, date, path, bys) = l
                    batch.add(insert_user, (uuid.uuid1(), host, date, path, int(bys)))
                    count = count +1
                if count == BATCH_SIZE:
                    session.execute(batch)
                    batch.clear()
                    count = 0

    session.execute(batch)

if __name__ == "__main__":
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(inputs,keyspace,table)


