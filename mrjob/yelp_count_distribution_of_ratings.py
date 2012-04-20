"""
Count the distribution of ratings

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRCountYelpBusinesses(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol
	
	def mapper(self, _, review):
		if review['type'] == 'review':
			rating = review['stars']
			yield rating, 1

	def reducer(self, rating, value):
		yield rating, sum(value)

if __name__ == '__main__':
	MRCountYelpBusinesses.run()