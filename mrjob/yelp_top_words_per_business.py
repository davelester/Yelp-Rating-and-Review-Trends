"""
Output list of top words for each business id

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from itertools import izip
import itertools
import re

class MRTopWordsPerBusiness(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol
	
	def mapper(self, _, review):
		if review['type'] == 'review':
			review_text = review['text']
			rating = review['stars']
			yield review['business_id'], (rating, review_text)

	def reducer(self, biz_id, ratings_and_review_texts):
		ratings, review_texts = izip(*ratings_and_review_texts)

		review_words = dict()
		for review_text in review_texts:
			review_text = review_text.lower()
			words = re.findall('\w+', review_text)
			
			for word in words:
				if word not in review_words:
					review_words[word] = 1
				else:
					review_words[word] += 1
		
		sorted(review_words.values())
		
		x = itertools.islice(review_words.items(), 0, 5)

		list_of_top_words = []
		for key, value in x:
		    list_of_top_words.append((key, value))
		
		yield biz_id, list_of_top_words

	def finale(self, key, value):
		yield key, list(value)

	def steps(self):
		return [self.mr(self.mapper, self.reducer),
				self.mr(reducer=self.finale)]

if __name__ == '__main__':
	MRTopWordsPerBusiness.run()