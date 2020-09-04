# Input: [[1,1],2,[1,1]]
# Output: [1,1,2,1,1]

a = [[1,1],2,[1,1]]

def flatenlist(input_list, store_list):

	for i in input_list:
		if isinstance(i, int):
			store_list.append(i)
		elif isinstance(i, list):
			flatenlist(i, store_list)
	return store_list

print(flatenlist(a, []))

# One liner solution for flattening "a list of lists"
# Input: [[1,1],[2],[1,1]]
# Output: [1,1,2,1,1]

b = [[1,1],[2],[1,1]]
print([j for i in b for j in i])