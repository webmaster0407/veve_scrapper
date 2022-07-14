import os
try:
	import requests
except:
	print("Installing missing libraries..")
	os.system("pip install requests")
finally:
	import requests
try:
	import sqlite3
except:
	print("Installing missing libraries..")
	os.system("pip install sqlite3")
finally:
	import sqlite3
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
dbname = "Records"
while os.path.exists(dbname+".db"):
	dbname+="_new"
conn = sqlite3.connect(f"{dbname}.db")
conn.execute("DROP TABLE IF EXISTS Transactions;")
conn.execute('''CREATE TABLE Transactions
		(TxnID INT PRIMARY KEY NOT NULL,
		TxnTime INT NOT NULL,
		TxnType TEXT NOT NULL,
		From_ CHAR(50),
		To_ CHAR(50),
		TokenAddress CHAR(50),
		TokenID CHAR(50));''')
counter=0
iterator=1

with open("errors_log.txt", "w") as errors:
	response = requests.post("https://3vkyshzozjep5ciwsh2fvgdxwy.appsync-api.us-west-2.amazonaws.com/graphql", 
			headers={'Accept': 'application/json, text/plain, */*', 'Content-Type': 'application/json', 
			'Origin': 'https://immutascan.io', 'Referer': 'https://immutascan.io/', 
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36', 
			'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"', 'sec-ch-ua-mobile': '?0', 
			'sec-ch-ua-platform': '"Windows"', 'x-api-key': 'da2-ihd6lsinwbdb3e6c6ocfkab2nm'},
			json={"operationName":"listTransactionsV2","variables":{"address":"0xa7aefead2f25972d80516628417ac46b3f2604af","pageSize":2500,"txnType":""},"query":"query listTransactionsV2($address: String!, $pageSize: Int, $nextToken: String, $txnType: String, $maxTime: Float) {\n  listTransactionsV2(\n    address: $address\n    limit: $pageSize\n    nextToken: $nextToken\n    txnType: $txnType\n    maxTime: $maxTime\n  ) {\n    items {\n      txn_time\n      txn_id\n      txn_type\n      transfers {\n        from_address\n        to_address\n        token {\n          type\n          quantity\n          usd_rate\n          token_address\n          token_id\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    nextToken\n    lastUpdated\n    txnType\n    maxTime\n    scannedCount\n    __typename\n  }\n}"}, 
			verify=False)
	code = response.status_code
	print(f"Response Code: {code}.")
	if code == 200 or code == "200":
		data = response.json()['data']["listTransactionsV2"]
		items = data['items']
		scannedcount = data['scannedCount']
		next_token = data['nextToken']
		print(f"Scanned Count: {scannedcount}.")
		if items:
			for it in items:
				txn_time = it['txn_time']
				txn_id = it['txn_id']
				txn_type = it['txn_type']
				transfers = it['transfers'][0]
				from_address = transfers['from_address']
				to_address = transfers['to_address']
				token_address = transfers['token']['token_address']
				token_id = transfers['token']['token_id']
				params = (txn_id, txn_time, txn_type, from_address, to_address, token_address, token_id)
				conn.execute(f'''INSERT INTO Transactions VALUES (?, ?, ?, ?, ?, ?, ?);''', params)
			conn.commit()
			items_length = len(items)

		else:
			with open(f"errors\\error{iterator}.txt", "w", encoding='utf-8') as f:
				f.write(f"Iteration: {iterator}\nScanned Count: {scannedcount}\nStatus Code: {code}\nResponse:\n{resp.text}\n")
			items_length=0

		counter+=items_length
		print(f"Got {items_length} Items.")
		print(f"\nGot Total {counter} Transactions.\n")

	else:
		errors.write(f"Error at {iterator} iteration with status code: {code}")
	iterator+=1

	while True:
		resp = requests.post(url="https://3vkyshzozjep5ciwsh2fvgdxwy.appsync-api.us-west-2.amazonaws.com/graphql", 
			headers={'Accept': 'application/json, text/plain, */*', 'Content-Type': 'application/json', 
			'Origin': 'https://immutascan.io', 'Referer': 'https://immutascan.io/', 
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36', 
			'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"', 'sec-ch-ua-mobile': '?0', 
			'sec-ch-ua-platform': '"Windows"', 'x-api-key': 'da2-ihd6lsinwbdb3e6c6ocfkab2nm'}, 
			json={"operationName":"listTransactionsV2","variables":{"address":"0xa7aefead2f25972d80516628417ac46b3f2604af","pageSize":2500,"nextToken":f"{next_token}","txnType":""},"query":"query listTransactionsV2($address: String!, $pageSize: Int, $nextToken: String, $txnType: String, $maxTime: Float) {\n  listTransactionsV2(\n    address: $address\n    limit: $pageSize\n    nextToken: $nextToken\n    txnType: $txnType\n    maxTime: $maxTime\n  ) {\n    items {\n      txn_time\n      txn_id\n      txn_type\n      transfers {\n        from_address\n        to_address\n        token {\n          type\n          quantity\n          usd_rate\n          token_address\n          token_id\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    nextToken\n    lastUpdated\n    txnType\n    maxTime\n    scannedCount\n    __typename\n  }\n}"}, 
			verify=False)
		code = resp.status_code
		print(f"Response Code: {resp.status_code}.")
		if code == 200 or code == "200":
			data = resp.json()['data']["listTransactionsV2"]
			items = data['items']
			scannedcount = data['scannedCount']
			next_token = data['nextToken']
			print(f"Scanned Count: {scannedcount}.")
			if items:
				for it in items:
					txn_time = it['txn_time']
					txn_id = it['txn_id']
					txn_type = it['txn_type']
					transfers = it['transfers'][0]
					from_address = transfers['from_address']
					to_address = transfers['to_address']
					token_address = transfers['token']['token_address']
					token_id = transfers['token']['token_id']
					params = (txn_id, txn_time, txn_type, from_address, to_address, token_address, token_id)
					conn.execute(f'''INSERT INTO Transactions VALUES (?, ?, ?, ?, ?, ?, ?);''', params)
				conn.commit()
				items_length = len(items)
			else:
				with open(f"errors\\error{iterator}.txt", "w", encoding='utf-8') as f:
					f.write(f"Iteration: {iterator}\nScanned Count: {scannedcount}\nStatus Code: {code}\nResponse:\n{resp.text}\n")
				items_length=0

			counter+=items_length
			print(f"Got {items_length} Items.")
			print(f"\nGot Total {counter} Transactions.\n")
		else:
			errors.write(f"Error at {iterator} iteration with status code: {code}")
		iterator+=1
print(f"Done Scraping {counter} Records.")
conn.close()