import MapReduce
import sys

"""
Join Example in the Simple Python MapReduce Framework
"""
#initializing the Map Reduce object
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: id
    # value: table row
    key = record[1]
    value = record
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: id
    # value: list of table rows with that id
    total = []
    order = []
    line_item = []
    for v in list_of_values:
        if len(v) == 10:
	    # this means v belongs to one of the tables
            order.append(v)
        else:
	    # this measn v belongs to the otehr table
            line_item.append(v)
    for r_order in order:
        for r_line_item in line_item:
	    # appending the rows from two tables 
	    # the common attribute is the key input of this reducer phase - all the items with the same keys will be sent to a reducer together
            mr.emit(r_order + r_line_item)        
    
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
