import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

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
            order.append(v)
        else:
            line_item.append(v)
    for r_order in order:
        for r_line_item in line_item:
            mr.emit(r_order + r_line_item)        
    
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
