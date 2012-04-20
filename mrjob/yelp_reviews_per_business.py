"""
Output list of review text for each unique business ID

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from itertools import izip

class MRReviewsPerBusinesses(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol
	
	def mapper(self, _, review):
		if review['type'] == 'review':
			yield review['business_id'], review['text']

	def reducer(self, biz_id, review_texts):

		for review_text in review_texts:
			yield biz_id, review_text

	def finale(self, key, value):
		yield key, list(value)

	def steps(self):
		return [self.mr(self.mapper, self.reducer),
				self.mr(reducer=self.finale)]

if __name__ == '__main__':
	MRReviewsPerBusinesses.run()