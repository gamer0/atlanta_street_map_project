import sqlite3
import re



def zip_correction():
  """ correct zipcodes with a range by stripping of the range part of zipcode.
  Then update database with new zipcode. """

  c.execute("select * from node_tags where key='addr:postcode' and value like '%-%'")
  resultset = c.fetchall()
  update_list = []

  for i in resultset:
    zip = i[3].split("-")
    zip = zip[0]
    tup = (zip,i[0])
    update_list.append(tup)

  c.executemany("update node_tags set value=? where node_tags_id=?", update_list)
  db.commit()
  return

def addr_correction():
  """ correct city designation inconsistency with regards to city values. Specifically correcting
  instances where first letter of each word in a city is lowercase to uppercase for consistency"""

  c.execute("select * from node_tags where key='addr:city'")
  resultset = c.fetchall()

  for i in resultset:
    if " " in i[3]:
      city_name = i[3].split()
      new_city_name = []
      for name in city_name:
        if name[0].isupper(): pass
        else: new_city_name.append(name[0].upper() + name[1:])
      c.execute("update node_tags set value=? where node_tags_id=?", (" ".join(new_city_name),i[0]))

    else:
      if i[3][0].isupper(): pass
      else:
        new_city_name = i[3][0].upper() + i[3][1:]
        c.execute("update node_tags set value=? where node_tags_id=?", (new_city_name,i[0]))
  db.commit()

  return




if __name__ == "__main__":
  database = "osm.db"
  db = sqlite3.connect(database)
  c = db.cursor()

  zip_correction()
  addr_correction()

  db.close()
