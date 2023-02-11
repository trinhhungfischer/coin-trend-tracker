def map_to_percentages(array):
    total = sum(array)
    return [round(x / total * 100, 2) for x in array]

reference = [2, 1, 3, 0]
array1 = ['c', 'a', 'd', 'b']
array2 = [300, 100, 400, 200]

# sorted_reference, sorted_array1, sorted_array2 = sort_arrays_by_reference(reference, array1, array2)

# print(sorted_reference) # [0, 1, 2, 3]
# print(sorted_array1) # ['b', 'a', 'c', 'd']
# print(sorted_array2) # [200, 100, 300, 400]
print(map_to_percentages(reference))