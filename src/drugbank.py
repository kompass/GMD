import untangle

if __name__ == '__main__':
	xmldoc = untangle.parse('Data/drugbank1.xml')

	for drug in xmldoc.root.child['drug']:
		print(drug['indication'])