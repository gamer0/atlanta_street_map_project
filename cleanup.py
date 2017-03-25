"""Dataset Cleanup"""

import xml.etree.cElementTree as ET
import re
import unicodecsv as csv
import sqlite3


# function to convert an xml element into a dictionary
def conv_to_dict(element):
  dict = {}
  way_attributes = ["id", "user", "uid", "version", "changeset", "timestamp"]
  node_attributes = (["id", "lat", "lon", "user", "uid","version", "changeset",
                      "timestamp"])
  tag_attributes = ["k", "v"]
  relation_attributes = (["id", "user", "uid", "version",
                          "changeset", "timestamp"])
  member_attributes = ["type", "ref", "role"]


  if element.tag == "node":
    for attribute in node_attributes:
      dict[attribute] = element.attrib[attribute]
    return dict
  elif element.tag == "tag":
    for attribute in tag_attributes:
      dict[attribute] = element.attrib[attribute]
    return dict
  elif element.tag == "way":
    for attribute in way_attributes:
      dict[attribute] = element.attrib[attribute]
    return dict
  elif element.tag == "nd":
    dict["ref"] = element.attrib["ref"]
    return dict
  elif element.tag == "relation":
    for attribute in relation_attributes:
      dict[attribute] = element.attrib[attribute]
    return dict
  elif element.tag == "member":
    for attribute in member_attributes:
      dict[attribute] = element.attrib[attribute]
    return dict
  else: print("Encountered problem")


# function to create a data structure for top level elements and their subelements
def data_structure(elem):
  if elem.tag == "node":
    ds = {}; tag = []
    ds["node"] = conv_to_dict(elem)
    for e in elem:
      tag.append(conv_to_dict(e))
    ds["tag"] = tag
    return ds
  elif elem.tag == "way":
    ds = {}; tag = []; nd = []
    ds["way"] = conv_to_dict(elem)
    for e in elem:
      if e.tag == "tag":
        tag.append(conv_to_dict(e))
      elif e.tag == "nd":
        nd.append(conv_to_dict(e))
      else:
        continue
    ds["tag"] = tag
    ds["nd"] = nd
    return ds
  elif elem.tag == "relation":
    ds = {}; tag = []; member = []
    ds["relation"] = conv_to_dict(elem)
    for e in elem:
      if e.tag == "tag":
        tag.append(conv_to_dict(e))
      elif e.tag == "member":
        member.append(conv_to_dict(e))
    ds["tag"] = tag
    ds["member"] = member
    return ds
  else: print("Encountered problem")

# function to look at each data structure and determine if
# data structure is valid (i.e each attribut value matches constraints outlined in document).
def validate_dict(adict):
  if elem.tag == "node":
    n = adict["node"]
    t = adict["tag"]
    tracker = {}
    tag_tracker = {}
    tag_tracker_list = []

    if re.search(r"\d+", n["id"], re.IGNORECASE): pass
    else: tracker["id"] = "yes"
    if re.search(r"\d\d.\d+", n["lat"], re.IGNORECASE): pass
    else: tracker["lat"] = "yes"
    if re.search(r"\d\d.\d+", n["lon"], re.IGNORECASE): pass
    else: tracker["lon"] = "yes"
    if re.search(r"\w+", n["user"], re.IGNORECASE): pass
    else: tracker["user"] = "yes"
    if re.search(r"\d+", n["uid"], re.IGNORECASE): pass
    else: tracker["uid"] = "yes"
    if re.search(r"\d+", n["version"], re.IGNORECASE): pass
    else: tracker["version"] = "yes"
    if re.search(r"\d+", n["changeset"], re.IGNORECASE): pass
    else: tracker["changeset"] = "yes"
    if re.search(r"\d+-\d+-\d+T\d\d:\d\d:\d\d\w", n["timestamp"], re.IGNORECASE): pass
    else: tracker["timestamp"] = "yes"

    if t is not None:
      for a in t:
        if re.search(r"\w+", a["k"], re.IGNORECASE): pass
        else: tag_tracker_list.append(a["k"])
        if a["v"]: pass

    if tracker: return {"node_issue": tracker, "k tag_issues": tag_tracker_list}
    elif tag_tracker_list: return {"node_issue": tracker, "k tag_issues": tag_tracker_list}
    else: return "no problem"

  if elem.tag == "way":
    w = adict["way"]
    t = adict["tag"]
    nd = adict["nd"]
    tracker = {}
    tag_tracker_list = []
    nd_tracker_list = []


    if re.search(r"\d+", w["id"], re.IGNORECASE): pass
    else: tracker["id"] = "yes"
    if re.search(r"\w+", w["user"], re.IGNORECASE): pass
    else: tracker["user"] = "yes" 
    if re.search(r"\d+", w["uid"], re.IGNORECASE): pass
    else: tracker["uid"] = "yes" 
    if re.search(r"\d+", w["version"], re.IGNORECASE): pass
    else: tracker["version"] = "yes" 
    if re.search(r"\d+", w["changeset"], re.IGNORECASE): pass
    else: tracker["changeset"] = "yes" 
    if re.search(r"\d+-\d+-\d+T\d\d:\d\d:\d\d\w", w["timestamp"], re.IGNORECASE): pass
    else: tracker["timestamp"] = "yes" 

    if t is not None:
      for a in t:
        if re.search(r"\w+", a["k"], re.IGNORECASE): pass
        else: tag_tracker_list.append(a["k"])
        if a["v"]: pass

    if nd is not None:
      for a in nd:
        if re.search(r"\d+", a["ref"], re.IGNORECASE): pass
        else: nd_tracker_list.append(a["ref"])

    if tracker:
      return ({"way_issue": tracker, "k tag_issues": tag_tracker_list, "nd tag_issues":
                    nd_tracker_list})
    if tag_tracker_list:
      return ({"way_issue": tracker, "k tag_issues": tag_tracker_list, "nd tag_issues":
                    nd_tracker_list})
    if nd_tracker_list:
      return ({"way_issue": tracker, "k tag_issues": tag_tracker_list, "nd tag_issues":
                    nd_tracker_list})
    else:
      return "no problem"



  if elem.tag == "relation":
    m_count = 0
    r = adict["relation"]
    t = adict["tag"]
    m = adict["member"]
    tracker = {}
    tag_tracker_list = []
    member_tracker = {}
    member_tracker_list = []

    if re.search(r"\d\d\d\d\d\d+", r["id"], re.IGNORECASE): pass
    else: tracker["id"] = "yes" 
    if re.search(r"\w+", r["user"], re.IGNORECASE): pass
    else: tracker["user"] = "yes"
    if re.search(r"\d+", r["uid"], re.IGNORECASE): pass
    else: tracker["uid"] = "yes"
    if re.search(r"\d+", r["version"], re.IGNORECASE): pass
    else: tracker["version"] = "yes"
    if re.search(r"\d+", r["changeset"], re.IGNORECASE): pass
    else: tracker["changeset"] = "yes"
    if re.search(r"\d+-\d+-\d+T\d\d:\d\d:\d\d\w", r["timestamp"], re.IGNORECASE): pass
    else: tracker["timestamp"] = "yes"

    if t is not None:
      for a in t:
        if re.search(r"\w+", a["k"], re.IGNORECASE): pass
        else: tag_tracker_list.append(a["k"])
        if a["v"]: pass
    if m is not None:
      for a in m:
        if re.search(r"\w+", a["type"], re.IGNORECASE): pass
        else: member_tracker["type"] = a["type"]
        if re.search(r"\d+", a["ref"], re.IGNORECASE): pass
        else:  member_tracker["ref"] = a["ref"]
        if a["role"]: pass
        member_tracker_list.append(member_tracker)

    if tracker:
      return ({"relation issues": tracker, "relation tag issues": tag_tracker_list, 
                   "member tag issues": member_tracker_list})
    if tag_tracker_list:
      return ({"relation issues": tracker, "relation tag issues": tag_tracker_list, 
                   "member tag issues": member_tracker_list})
    if member_tracker_list:
      return ({"relation issues": tracker, "relation tag issues": tag_tracker_list, 
                   "member tag issues": member_tracker_list})
    else: return "no problem"


# function writes data structure to csv file
def write_csv(data_structure):
  if elem.tag == "node":
    with open("nodes.csv", "ab") as nodefile, open("ntag.csv", "ab") as tagfile:
      n_writer = csv.writer(nodefile, delimiter=",")

      ds_node = data_structure["node"]
      ds_tag = data_structure["tag"]
      n_list = []
      for a in ds_node.keys(): n_list.append(ds_node[a])
      n_writer.writerow(n_list)

      if ds_tag is not None:
        t_writer = csv.writer(tagfile, delimiter=",")
        for a in ds_tag:
          l = [ds_node["id"]]
          for b in a.keys():
            l.append(a[b])
          t_writer.writerow(l)

  if elem.tag == "way":
    with open("ways.csv", "ab") as wayfile, open("wtag.csv", "ab") \
          as tagfile, open("nd.csv", "ab") as ndfile:
      w_writer = csv.writer(wayfile, delimiter=",")

      ds_way = data_structure["way"]
      ds_tag = data_structure["tag"]
      ds_nd = data_structure["nd"]
      w_list = []

      for a in ds_way.keys(): w_list.append(ds_way[a])
      w_writer.writerow(w_list)

      if ds_tag is not None:
        t_writer = csv.writer(tagfile, delimiter=",")
        for a in ds_tag:
          l = [ds_way["id"]]
          for b in a.keys():
            l.append(a[b])
          t_writer.writerow(l)

      if ds_nd is not None:
        nd_writer = csv.writer(ndfile, delimiter=",")
        for a in ds_nd:
          l = [ds_way["id"]]
          for b in a.keys():
            l.append(a[b])
          nd_writer.writerow(l)


  if elem.tag == "relation":
    with open("relations.csv", "ab") as relationfile, open("rtag.csv", "ab") \
          as tagfile, open("member.csv", "ab") as memfile:
      r_writer = csv.writer(relationfile, delimiter=",")

      ds_relation = data_structure["relation"]
      ds_tag = data_structure["tag"]
      ds_mem = data_structure["member"]
      r_list = []

      for a in ds_relation.keys(): r_list.append(ds_relation[a])
      r_writer.writerow(r_list)

      if ds_tag is not None:
        t_writer = csv.writer(tagfile, delimiter=",")
        for a in ds_tag:
          l = [ds_relation["id"]]
          for b in a.keys():
            l.append(a[b])
          t_writer.writerow(l)

      if ds_mem is not None:
        mem_writer = csv.writer(memfile, delimiter=",")
        for a in ds_mem:
          l = [ds_relation["id"]]
          for b in a.keys():
            l.append(a[b])
          mem_writer.writerow(l)
  return



if __name__ == "__main__":

  dataset = "atlanta_georgia.osm"
  for event, elem in ET.iterparse(dataset, events=("start",)):
    if elem.tag == "node" or elem.tag == "way" or elem.tag == "relation":
      j = data_structure(elem)
      q = validate_dict(j)
      if q == "no problem": write_csv(j)
      else: print(q)
    count += 1
    elem.clear()


#----------end of program----------------------






