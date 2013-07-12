import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
def mapper(record):
    # key: friendship receiver
    # value: friendship giver
    # the idea is to create virtual friendships..if such a virtual friendship is not duplicated, then it means the the friendship is assymetrical ooyeah think abt it
    key = record[0]
    value = record[1]
    #words = value.split()
    #for w in words:
    mr.emit_intermediate(key, value)
    mr.emit_intermediate(value, key)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #total = 0
    for v in list_of_values:
        if list_of_values.count(v) == 1:
            mr.emit((key, v))
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
