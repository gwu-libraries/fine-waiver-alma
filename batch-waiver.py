import pandas as pd
import json
import asyncio
from async_fetch import run_batch
import yaml
import os
import sys
from csv import DictWriter
from datetime import datetime

# CONFIG_FILE should by a YAML document, where inst_key is an institution name passed as a command line argument to the script, corresponding to a dict of attributes (apikey, data_file).
# CONFIG_FILE should also contain a list of policy names for which we want to waive fines.
CONFIG_FILE = 'config.yml'
POST_URL = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/users/{user_id}/fees/{fee_id}'
HEADERS = {'Authorization': 'apikey {}',
          'Accept': 'application/json'}
CSV_HEADERS = ['batch_number', 'total_succeeded', 'total_failed', 'institution', 'timestamp']

def pre_process(data, config, sandbox=False):
	'''Pre-process data according to parameters for this project.
	Data argument should be a pandas DataFrame.'''
	# Pre-migration fines have a null policy name. Set this to a default value
	data.loc[data['Policy Name'].isnull(), 'Policy Name'] = 'Voyager'
	to_waive = data.loc[data['Policy Name'].isin(config['policy_names'])].copy()
	if sandbox:
		# For testing, need to limit to fines in the sandbox
		to_waive['Fine Fee Creation Date'] = pd.to_datetime(to_waive['Fine Fee Creation Date'])
		to_waive = to_waive.loc[to_waive['Fine Fee Creation Date'] <= pd.to_datetime(config['sandbox_date'])].copy()
	# Isolate just the columns we need
	to_waive = to_waive.loc[:, ['Fine Fee Id', 'Remaining Amount', 'Primary Identifier']].copy()
	# Rename in order to match the parameter names used by the API
	to_waive.columns = ['fee_id', 'amount', 'user_id']
	return to_waive

def update_params(param_dict):
	'''Updates the parameters for the URL based on the passed values.'''
	params = {'op': 'waive',
			  'reason': 'OTHER',
			  'amount': str(param_dict['amount'])}
	return params

def update_log(responses, batch_num, inst_key):
	'''Accepts a list of responses where each element is a dict with keys "url" and "response". 
	Updates a CSV (named with the inst_key parameter) to record successes and failures.'''
	# Failures will have an error code for the response
	successes = len([response['response']['id'] for response in responses if isinstance(response['response'], dict)])
	failures = len(responses) - successes
	with open('./batch_log.csv', 'a') as f:
		writer = DictWriter(f, CSV_HEADERS)
		writer.writerow({'batch_number': batch_num,
						'total_succeeded': successes,
						'total_failed': failures,
						'institution': inst_key,
						'timestamp': datetime.now().strftime('%m-%d-%Y %H:%M')}) 

def waive_fines(inst_key, test=False):
	'''Loads data from a file specified in the config file for a given institution, then runs the batch update on that data to waive user fines.
	If test=True, then run on an isolated batch first.'''
	# Open the config file
	with open(CONFIG_FILE, 'r') as f:
		config = yaml.load(f)
	inst_data = config['institutions'][inst_key]
	# Load the user fine report for this institution
	data = pd.read_csv(inst_data['data_file'])
	# Get the API key for this institution
	api_key = inst_data['api_key']
	# Is this for the sandbox? If so, need to restrict the date range
	if inst_key == 'sandbox':
		sandbox = True
	else:
		sandbox = False
	fines_to_waive = pre_process(data, config, sandbox)
	# Convert this DataFrame to a list of dicts for ease of processing
	fines_to_waive = [i._asdict() for i in fines_to_waive.itertuples(index=False)]
	loop = asyncio.get_event_loop()
	# If it doesn't exist, make a directory for the results from this institution API call
	if not os.path.exists('./{}'.format(inst_key)):
		os.mkdir('./{}'.format(inst_key))
	if test:
		fines_to_waive = fines_to_waive[:100]
	# Add the API key to the head
	HEADERS['Authorization'] = HEADERS['Authorization'].format(api_key)
	# Run the batched updates, saving the results in a local directory
	for i, batch in enumerate(run_batch(loop, 
										fines_to_waive, 
										update_params, 
										POST_URL, 
										HEADERS,
		  								'./{}'.format(inst_key),
		  								http_type='POST')):
		update_log(batch, i, inst_key)

if __name__ == '__main__':
	# Institution code (or sandbox) should be the first argument to the command line script 
	inst_code = sys.argv[1]
	if (len(sys.argv) > 2) and (sys.argv[2] == 'test'):
		waive_fines(inst_code, True)
	else:
		waive_fines(inst_code, False)

	
