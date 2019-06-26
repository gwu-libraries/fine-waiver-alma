import aiohttp 
import asyncio
from throttler import Throttler
import json
from pathlib import Path

def chunk_list(items, n): 
    '''Create a chunked list of size n. Last segment may be of length less than n.'''
    for i in range(0, len(items), n):  
        yield items[i:i + n] 

async def post_record(client, results, param_fn, base_url, headers, row):
    '''Makes a single async POST request, given one or more system ids. 
    client should be an instance of the aiohttp ClientSession class.
    param_fn should be a function that returns a dictionary of parameters for the URL, given the data passed in row. It can be the identity function if the passed data are meant to be part of the un-parametrized URL, in which case the string formatting function will add it.
    row should be a dictionary of the form {key: value} where the key corresponds to either to a parameter key or a placeholder in the base_url string, and value is the value to assign.'''
    params = param_fn(row)
    base_url = base_url.format(**row)
    async with client.post(base_url, params=params, headers=headers) as session:
        if session.status != 200:
            results.append({'url': str(session.url),
                    'response': session.status})
            return
        elif session.content_type == 'application/json':
            response = await session.json()
        else:
            response = await session.text()
    results.append({'url': str(session.url),
            'response': response}) 

async def fetch_record(client, results, param_fn, base_url, headers, row):
    '''Makes a single async request, given one or more system ids. 
    client should be an instance of the aiohttp ClientSession class.
    param_fn should be a function that returns a dictionary of parameters for the URL, given the data passed in row. It can be the identity function if the passed data are meant to be part of the un-parametrized URL, in which case the string formatting function will add it.
    row should be a dictionary of the form {key: value} where the key corresponds to either to a parameter key or a placeholder in the base_url string, and value is the value to assign.'''
    params = param_fn(row)
    base_url = base_url.format(**row)
    async with client.get(base_url, params=params, headers=headers) as session:
        if session.status != 200:
            results.append({'url': str(session.url),
                    'response': session.status})
            return
        elif session.content_type == 'application/json':
            response = await session.json()
        else:
            response = await session.text()
    results.append({'url': str(session.url),
            'response': response})

async def throttle_request(throttler, async_fn, *args, **kwargs):
    '''Throttles the request. This allows us to re-use the clientsession on each call. '''
    async with throttler:
        return await async_fn(*args, **kwargs)

async def get_records(loop, ids, results, *args, rate_limit=25, http_type='GET'):
    '''From a list of system id's, makes async requests to retrieve the data. 
    loop should be an instance of the asyncio event loop.
    ids should be a list, used to generate requests, with a URL parametrized by param_fn.
    results should be a list to which response data will be added, one at a time.
    rate limit value is used to throttle the calls to a specified rate per second.
    http_type is used to determine which async aiohttp method to use: GET or POST.'''
    throttler = Throttler(rate_limit=rate_limit)
    if http_type == 'GET':
        async_fn = fetch_record
    else:
        async_fn = post_record
    async with aiohttp.ClientSession() as client:
        awaitables = [loop.create_task(throttle_request(throttler, 
                                                      async_fn,
                                                      client,
                                                      results, 
                                                      *args,
                                                      row=row)) for row in ids]
        await asyncio.gather(*awaitables)
    return len(results)

def run_batch(loop, ids, param_fn, base_url, headers, path_to_files, batch_size=1000, http_type='GET'):
    '''Runs an async fetch in batches set by batch_size, saving the results in JSON format to the specified path.
    param_fn should be a function for parametrized base_url based on each id in ids.
    ids should be a list of dictionaries, each dict containing one or more key-value pairs for constructing the URL.'''
    path_to_files = Path(path_to_files)
    for i, batch in enumerate(chunk_list(ids, batch_size)):
        # Reset the results each time through
        results = []
        # Run the loop on the current batch
        loop.run_until_complete(get_records(loop, batch, results, param_fn, base_url, headers, http_type=http_type))
        # Print the first 1000 characters of the last response, in case it's an error message
        print("Head of last result: {}".format(json.dumps(results[-1])[:1000]))
        # Write this batch to the disk
        print("Saving batch {} to disk".format(i))
        with open(path_to_files / 'results_batch-{}.json'.format(i), 'w') as f:
            json.dump(results, f)
        # Yield the batch to the caller for further processing
        yield results