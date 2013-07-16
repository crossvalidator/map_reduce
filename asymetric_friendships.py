import MapReduce
import sys

"""
Displaying assymetric friendships in the Simple Python MapReduce Framework
"""
#initializing the Map Reduce object
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
def mapper(record):
    # key: friendship receiver
    # value: friendship giver
    # the idea is to create virtual friendships..if such a virtual friendship is not duplicated, then it means the the friendship is assymetrical 
    key = record[0]
    value = record[1]
    
    mr.emit_intermediate(key, value)
    mr.emit_intermediate(value, key)

def reducer(key, list_of_values):
    # key: person identifier
    # value: list of possible friends - assymetric or symmetric yet to be determined
    
    for v in list_of_values:
        if list_of_values.count(v) == 1: 
	    # this means one of (key, v) or (v, key) does nto appear in the original list - so friendship is assymetrical 
            mr.emit((key, v))
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
