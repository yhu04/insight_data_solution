__author__ = 'YueHu'
import sys
from copy import deepcopy

class Payment:
	def __init__(self, payment_str):
		temp = payment_str.split(",")
		self.time = temp[0]
		self.payer_id = int(temp[1])
		self.payee_id = int(temp[2])
		self.amount = temp[3]


class PaymentVerifier:
	"""
	the class is built to verify payments and check the possible fraud 
	"""
	def __init__(self, payment_history):
		print("building graphs")
		self.build_graphs(payment_history)
		self.deg_3_cache = {}
		print("Finishing initialization")

	# build graph from the historical data, only build base graph and the graph
	# with degree=1. Graph with degree 3 is not built here, because building it
	# is quite slow, thus we use bfs during query time and cache the results.
	def build_graphs(self, payment_history):
		print("building base graph")
		self.basegraph = {}
		with open(payment_history) as f:
			next(f)
			for line in f:
				payment = Payment(line)
				if payment.payer_id in self.basegraph:
					self.basegraph[payment.payer_id].add(payment.payee_id)
				else:
					self.basegraph[payment.payer_id]=set([payment.payee_id])
				if payment.payee_id in self.basegraph:
					self.basegraph[payment.payee_id].add(payment.payer_id)
				else:
					self.basegraph[payment.payee_id]=set([payment.payer_id])
		print("building deg1 graphs")
		self.deg_1_graph = deepcopy(self.basegraph)
		for i in self.basegraph:
				for j in self.basegraph[i]:
					for k in self.basegraph[j]:
						if k not in self.deg_1_graph[i] and k is not i:
							self.deg_1_graph[i].add(k)

	# Get degree 3 neighbors, if the id hits cache, cache will be returned,
	# otherwise, performan bfs and cache the result.
	def compute_cache(self, id):
		self.deg_3_cache[id] = deepcopy(self.basegraph[id])
		last_new_elements = deepcopy(self.basegraph[id])
		new_elements = set()
		deg = 0
		while deg < 3:
			for i in last_new_elements:
				for k in self.basegraph[i]:
					if k not in self.deg_3_cache[id] and k is not id:
						self.deg_3_cache[id].add(k)
						new_elements.add(k)
			last_new_elements = new_elements
			new_elements = set()
			deg += 1
		return self.deg_3_cache[id]

	# check if the transaction is verified
	def verify_payment(self, payment, method):
		if payment.payer_id not in self.basegraph or payment.payee_id not in self.basegraph:
			return "unverified"
		if method is "deg_0":
			return "trusted" if payment.payee_id in self.basegraph[payment.payer_id] else "unverified"
		elif method is "deg_1":
			return "trusted" if payment.payee_id in self.deg_1_graph[payment.payer_id] else "unverified"
		elif method is "deg_3":
			if payment.payee_id in self.deg_1_graph[payment.payer_id]:
				return "trusted"
			if payment.payer_id in self.deg_3_cache:
				return "trusted" if payment.payee_id in self.deg_3_cache[payment.payer_id] else "unverified"
			if payment.payee_id in self.deg_3_cache:
				return "trusted" if payment.payer_id in self.deg_3_cache[payment.payee_id] else "unverified"
			return "trusted" if payment.payee_id in self.compute_cache(payment.payer_id) else "unverified"
		else:
			raise ValueError("Invalid verification method.")


def main(batch_input,stream_file,output1,output2,output3):
	payment_verifier = PaymentVerifier(batch_input)
	result1 = []
	result2 = []
	result3 = []
	print('Starting to processing streaming payments')
	count = 0
	with open(stream_file) as f:
		next(f)
		for line in f:
			payment = Payment(line)
			result1.append(payment_verifier.verify_payment(payment, "deg_0"))
			result2.append(payment_verifier.verify_payment(payment, "deg_1"))
			result3.append(payment_verifier.verify_payment(payment, "deg_3"))
			count += 1
			if count % 1000 == 0:
				print(str(count) + " payments processed.")
	open(output1, 'w').write('\n'.join(result1))
	open(output2, 'w').write('\n'.join(result2))
	open(output3, 'w').write('\n'.join(result3))
	

if __name__ == '__main__':
	main(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5])

