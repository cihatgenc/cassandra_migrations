import sys
import os
from natsort import natsorted, ns
from cassandra.cluster import Cluster

print (sys.version)
print ("Running cassandra migrations....")

print ("Reading environment variables")
cassandra_host = os.getenv('CASSANDRA_HOST', '127.0.0.1')
cassandra_port = os.getenv('CASSANDRA_PORT', 9042)
cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'testkeyspace')
project_path = os.getenv('CI_PROJECT_DIR')

print ("CASSANDRA_HOST=", cassandra_host)
print ("CASSANDRA_PORT=", cassandra_port)
print ("CASSANDRA_KEYSPACE=", cassandra_keyspace)
print ("CI_PROJECT_DIR=", project_path)

migrationspath = os.path.join(project_path, "db", "migrations") 
print ("Path for migrations=", migrationspath)

cluster = Cluster(  
  contact_points=[cassandra_host],
  port=cassandra_port
)
session = cluster.connect()

try:
  print ("Creating keyspace ", cassandra_keyspace, "if it does not exist yet")
  createkeyspacequery= "".join(["CREATE KEYSPACE IF NOT EXISTS ", cassandra_keyspace, " WITH replication = {'class':'SimpleStrategy','replication_factor':1};"])
  createkeyspace = session.execute(createkeyspacequery)
  session.set_keyspace(cassandra_keyspace)

  print ("Creating table migration if it does not exist yet")
  createmigrationtable = session.execute("CREATE TABLE IF NOT EXISTS migrations (insertdate timestamp, script varchar, PRIMARY KEY (script));")

  # print ("Inserting test migrations in the table")
  # session.execute("INSERT INTO migrations (insertdate, script) VALUES (toTimestamp(now()), '0001-step1.cql')")
  # session.execute("INSERT INTO migrations (insertdate, script) VALUES (toTimestamp(now()), '0002-step2.cql')")
  # session.execute("INSERT INTO migrations (insertdate, script) VALUES (toTimestamp(now()), '0004-step4.cql')")

  unsorteddblist = []
  dbmigrations = session.execute("SELECT script FROM migrations")
  for scripts in dbmigrations:
    unsorteddblist.append(scripts.script)

  print ("Following migrations are found in the database")
  sorteddblist = natsorted(unsorteddblist)
  for scripts in sorteddblist:
    print (scripts)

  print ("Following migrations are found on filesystem")
  repomigrations = natsorted(os.listdir(migrationspath))
  for files in repomigrations:
    print (files)

  print ("Following migrations will be executed")
  sortedexecutelist = natsorted([item for item in repomigrations if item not in sorteddblist])
  for files in sortedexecutelist:
    print (files)

  for script in sortedexecutelist:
    fullpath = os.path.join(project_path, "db", "migrations", script) 
    input = open(fullpath, "r")
    query = input.read()
    print ("Executing script", fullpath)
    session.execute(query)
    print ("Adding script to migrations table")
    session.execute("INSERT INTO migrations (insertdate, script) VALUES (toTimestamp(now()), %s)", [script])
except:
  print("Unexpected error:", sys.exc_info()[0])
  raise

cluster.shutdown