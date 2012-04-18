"""
Count the number of reviews for each unique business ID

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRCountYelpReviewsPerBusinesses(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol
	
	def mapper(self, _, review):
		if review['type'] == 'review':
			biz_id = review['business_id']
			yield biz_id, 1

	def reducer(self, biz_id, value):
		num_of_reviews = sum(value)
		yield biz_id, num_of_reviews

if __name__ == '__main__':
	MRCountYelpReviewsPerBusinesses.run()