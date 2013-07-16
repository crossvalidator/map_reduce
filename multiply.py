import MapReduce
import sys

"""
Matrix multiplication example in the Simple Python MapReduce Framework
"""
#initializing the Map Reduce object
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: (i, k) pairs 
    # value: list of other indices and the matrix value at the said indices
    if record[0] == 'a':
        #entry is from matrix A
        i = record[1]
        for k in range(0, 5):
            mr.emit_intermediate((i, k), [record[0], record[2], record[3]])
    else:
        #Entry is from Matrix B
        k = record[2]
        for i in range(0, 5):
            mr.emit_intermediate((i, k), [record[0], record[1], record[3]])

def reducer(key, list_of_values):
    # key: (i, k) pairs
    # value: list of lists of matrix values and indices
    list_of_a = [x for x in list_of_values if x[0] == 'a']
    list_of_b = [x for x in list_of_values if x[0] == 'b']
    count = 0
    for a_element in list_of_a:
        for b_element in list_of_b:
            if a_element[1] == b_element[1]:
                count += a_element[2]*b_element[2]

    mr.emit([key[0], key[1], count])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
