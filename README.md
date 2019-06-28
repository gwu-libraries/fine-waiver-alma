# Python App for Waiving Fines in Alma

This app uses the Alma Fines/Fees API to waive fines for users based on an Analytics report. In order to facilitate its use in a Consortial environment, it supports waiving fines for multiple Institution Zones (as specified in a config YAML file).

API calls are asynchronous, throttled at 25 per second (per Ex Libris' documented rate limit). Furthermore, calls are made in batches, in order to make errors easier to locate and troubleshoot.

## Requirements, Installation, & Setup

- Requires Python 3.6 or higher.
- It is recommended to use `venv` or `conda` to create a virtual environment for running this script.
- You will need a Users API key with read/write privileges for each IZ.
- You will also need to run an Alma Analytics report for each IZ. The report template is `outstanding_fines_by_user_for_api`, available in the Alma Community folder > Reports > Consortia > WRLC > Reports > Fines and Fees.
- Clone this repo or download the files in this repo to a directory on your machine.
- Create and activate a new virtual environment. 
- Run `pip install -r requirements.txt` to install the project dependencies.
- Within the project directory, create a subfolder for each IZ for which you want to waive the fines. **The name of this folder must match the name provided in the `config.yml` file** (see below).
- Run and download the Analytics report -- in CSV format -- to the subdirectory for each IZ. 
- Edit `config.yml` as follows:
  - Include the API key and the path to the report for each IZ, following the examples provided. 
  - Edit the `policy_names` list as needed. Fines will be waived only for items with an item policy found in this list. Note that the policy name `Voyager` is applied by the script in cases where the Policy Name field in Alma is blank. (This occurs for fines migrated from our Voyager ILS.)

## Instructions for Use

Once `config.yml` has been updated, run the script from the command line as follows:
<pre>
python batch_waiver.py <i>institution_name</i> [test]
</pre>
The required argument _institution_name_ should match one of the keys under `institutions` in the `config.yml` file. If _institution_name_ is `sandbox` the script will waive only those fines recorded prior to or on the date of the last sandbox refresh (as defined in the `config.yml` file). 

If the optional argument `test` is provided, the script will use only the first 100 rows in the report. 

As each batch completes, API responses are saved to a new JSON file in the folder corresponding to _institution_name_, and the `batch_log.csv` file is updated to record the number of successes and failures for that batch. (A failure is recorded for a response with a status code other than 200.) The script also prints to standard output the last response received from the API for each batch (useful for testing/troubleshooting). 


