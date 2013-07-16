import MapReduce
import sys

"""
Inverted index Example in the Simple Python MapReduce Framework
"""
#initializing the Map Reduce object
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
    # key: word
    # value: list of document identifiers in which the word occurs
   
    # the same word could have appeard multiple times in a document - thus after the map phase, these could be duplicates
    no_duplicate_values = []

    # I have tried to maintain the same order other wise this could have been done by list(set(list_of_values))
    for v in list_of_values:
        if v not in no_duplicate_values:
            no_duplicate_values.append(v)

    mr.emit((key, no_duplicate_values))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
