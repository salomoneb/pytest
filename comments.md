## Contents
`index.py` contains a Python script which parses, merges, and sorts data from the provided CSV files, filling in the second-lowest cost silver plan for the zip codes in `slcsp.csv`. 
## To Run
1. Install [Python 3](http://docs.python-guide.org/en/latest/starting/install3/osx/) and [Pandas](https://pandas.pydata.org/) (if you don't already have them). 
2. With `index.py` in the same directory as `slcsp.csv`, `zips.csv`, and `plans.csv`, run `python3 index.py`. The correct rates will be populated in the "Rates" column of the `slcsp.csv` file.