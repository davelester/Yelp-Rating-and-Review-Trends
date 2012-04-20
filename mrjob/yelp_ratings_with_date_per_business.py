"""
Output a list of star ratings and date for each business ID

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from itertools import izip

class MRRatingsWithDatePerBusinesses(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol

	def mapper(self, _, review):
		if review['type'] == 'review':
			biz_id = review['business_id']
			rating = review['stars']
			date = review['date']
			yield biz_id, (rating, date)

	def reducer(self, biz_id, value):
		ratings, dates = izip(*value)

		counter = 0
		for rating in ratings:
			yield biz_id, (dates[counter], rating)
			counter += 1

	def finale(self, key, value):
		yield key, list(value)

	def steps(self):
		return [self.mr(self.mapper, self.reducer),
				self.mr(reducer=self.finale)]

if __name__ == '__main__':
	MRRatingsWithDatePerBusinesses.run()