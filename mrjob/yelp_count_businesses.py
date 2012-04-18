"""
Count the number of unique business IDs

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRCountYelpBusinesses(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol
	
	def mapper(self, _, review):
		if review['type'] == 'review':
			biz_id = review['business_id']
			yield 'business', biz_id

	def reducer(self, biz_id, value):
		num_of_biz = len(set(list(value)))
		yield 'number of businesses', num_of_biz

if __name__ == '__main__':
	MRCountYelpBusinesses.run()