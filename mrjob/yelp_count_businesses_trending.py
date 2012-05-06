"""
Out number of businesses that are trending.

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from itertools import izip
from operator import itemgetter, attrgetter
import numpy
import pylab
from scipy import stats

class MRRatingsLinearRegressionTrending(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol

	def mapper(self, _, review):
		if review['type'] == 'review':
			biz_id = review['business_id']
			rating = review['stars']
			date = review['date']
			yield biz_id, (rating, date)

	def reducer(self, biz_id, value):
		# sort list of tuples based upon their date
		value = sorted(value, key=itemgetter(1))
		ratings, dates = izip(*value)
		
		counter = 1
		x = []
		for rating in ratings:
			x.append(counter)
			counter += 1

		# run that linear regression!
		slope, intercept, r_value, p_value, std_err = stats.linregress(numpy.log2(x), ratings)
		
		# possible change/todo: whether or not a rating is trending may depend on the value of std_err

		if (slope > 0.6) and (len(x) >= 5):
			yield 'Number of trending businesses: ', 1
			
	def final(self, key, value):
		yield key, sum(value)

	def steps(self):
		return [self.mr(self.mapper, self.reducer),
				self.mr(reducer=self.final)]

if __name__ == '__main__':
	MRRatingsLinearRegressionTrending.run()