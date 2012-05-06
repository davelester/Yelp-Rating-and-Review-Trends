"""
Count the number of businesses for each unique category

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRCountBusinessesPerCategory(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol
	
	def mapper(self, _, review):
		if review['type'] == 'business':
			categories = review['categories']
			
			for category in categories:
				yield category, 1

	def reducer(self, category_name, value):
		num_of_businesses = sum(value)
		yield category_name, num_of_businesses

if __name__ == '__main__':
	MRCountBusinessesPerCategory.run()