import xml.etree.cElementTree as ET
import re
import unicodecsv as csv
import sqlite3


def tag_count():
  n = 0; w = 0; r = 0; nt = 0; wt = 0; nd = 0; m = 0; rt = 0
  for event, elem in ET.iterparse("atlanta_georgia.osm", events=("start",)):
    if elem.tag == "node":
      n += 1
      for e in elem:
        if e.tag == "tag": nt += 1
    elif elem.tag == "way":
      w += 1
      for e in elem:
        if e.tag == "tag": wt += 1
        if e.tag == "nd": nd += 1
    elif elem.tag == "relation":
      r += 1
      for e in elem:
        if e.tag == "tag": rt += 1
        if e.tag == "member": m += 1
    elem.clear()

  print("nodes: {}, node_tags: {}" .format(n, nt))
  print("ways: {}, way_tags: {}, nd_tags: {}" .format(w, wt, nd))
  print("relations: {}, relation_tags: {}, members: {}" .format(r, rt, m))



if __name__ == "__main__":
  tag_count()
