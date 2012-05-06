"""
List business ids associated with each unique category

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRListBusinessIdsPerCategory(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol
	
	def mapper(self, _, review):
		if review['type'] == 'business':
			categories = review['categories']
			business_id = review['business_id']
			
			for category in categories:
				yield category, business_id

	def reducer(self, category_name, value):
		yield category_name, list(value)

if __name__ == '__main__':
	MRListBusinessIdsPerCategory.run()