import unicodecsv as csv
import sqlite3
import re



def write2db(file):
  with open(file, "rU") as f:
    reader = csv.reader(f)

    if file == "nodes.csv":
      query = "insert into nodes (changeset,uid,timestamp,lon,version,user,lat,id) values (?,?,?,?,?,?,?,?)"
      nodes_list = []
      for i in reader: nodes_list.append(i)
      c.executemany(query, nodes_list)
    elif file == "ntag.csv":
      query = "insert into node_tags (id,key,value) values (?,?,?)"
      node_tags_list = []
      for i in reader: node_tags_list.append(i)
      c.executemany(query, node_tags_list)
    elif file == "ways.csv":
      query = "insert into ways (changeset,uid,timestamp,version,user,id) values (?,?,?,?,?,?)"
      ways_list = []
      for i in reader: ways_list.append(i)
      c.executemany(query, ways_list)
    elif file == "wtag.csv":
      query = "insert into way_tags (id,key,value) values (?,?,?)"
      way_tags_list = []
      for i in reader: way_tags_list.append(i)
      c.executemany(query, way_tags_list)
    elif file == "nd.csv":
      query = "insert into way_nodes (id,position) values (?,?)"
      nd_list = []
      for i in reader: nd_list.append(i)
      c.executemany(query, nd_list)
    elif file == "relations.csv":
      query = "insert into relations (changeset,uid,timestamp,version,user,id) values (?,?,?,?,?,?)"
      relations_list = []
      for i in reader: relations_list.append(i)
      c.executemany(query, relations_list)
    elif file == "rtag.csv":
      query = "insert into relation_tags (id,key,value) values (?,?,?)"
      relation_tags_list = []
      for i in reader: relation_tags_list.append(i)
      c.executemany(query, relation_tags_list)
    elif file == "member.csv":
      query = "insert into member_tags (id,ref,role,type) values (?,?,?,?)"
      member_tags_list = []
      for i in reader: member_tags_list.append(i)
      c.executemany(query, member_tags_list)
    else: print("file not recognized!")
  db.commit()
  return 0




if __name__ == "__main__":
  db = sqlite3.connect("/home/tchalla/project3/osm_db/osm.db")
  c = db.cursor()
  file_list = (["nodes.csv", "ntag.csv", "ways.csv", "wtag.csv", "nd.csv", "relations.csv",
                 "rtag.csv", "member.csv"])
  for i in file_list:
    write2db(i)
  db.close()
