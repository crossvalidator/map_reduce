import MapReduce
import sys

"""
Unique DNA sequence trims example in the Simple Python MapReduce Framework
This gives the DNA sequences consisting of all but last 10 characters - duplicates removed
"""
#initializing the Map Reduce object
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: DNA string id
    # value: DNA string
    key = record[0]
    value = record[1]
    mr.emit_intermediate(value[0:len(value)-10], 1)

def reducer(key, list_of_values):
    # key: all but last 10 letters of the DNA sequence
    # value: list of occurrence counts
    mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
