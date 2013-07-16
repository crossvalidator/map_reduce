import MapReduce
import sys

"""
Counting friends exmaple in the Simple Python MapReduce Framework
"""

#initializing the Map Reduce object
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: friend 1
    # value: friend 2
    key = record[0]
    value = record[1]
    
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: person identifier
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
