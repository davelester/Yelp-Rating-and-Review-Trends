"""
Count the number of unique business IDs for each school in the academic dataset.

"""

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRCountBusinessesPerSchool(MRJob):
	INPUT_PROTOCOL = JSONValueProtocol
	
	def mapper(self, _, review):
		if review['type'] == 'business':
			biz_id = review['business_id']
			school = review['schools']
			yield school, biz_id

	def reducer(self, school, value):
		num_of_biz = len(set(list(value)))
		yield str(school), num_of_biz

if __name__ == '__main__':
	MRCountBusinessesPerSchool.run()