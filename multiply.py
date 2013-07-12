import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    if record[0] == 'a':
        i = record[1]
        for k in range(0, 5):
            mr.emit_intermediate((i, k), [record[0], record[2], record[3]])
    else:
        k = record[2]
        for i in range(0, 5):
            mr.emit_intermediate((i, k), [record[0], record[1], record[3]])

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
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
