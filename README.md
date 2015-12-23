# Daily Fantasy Recommender
###### Columbia University
<small>Created On: 11.20.2015</small><br/>
<small>Last Updated: 12.21.2015</small><br/>
<small>Authors: [Justin M Pugliese](mailto:jp3571@columbia.edu), [Michael A. Raimi](mailto:mar2260@columbia.edu)</small>


## Introduction

Fantasy sport gaming participation has been rapidly increasing over the past several years in several forms.  The latest adaptation, Daily Fantasy, has seen a sharp spike in popularity as several gaming platforms have enabled players to compete for cash.

The promise of a cash reward has caused several government agencies to start investigating the amount of skill involved with succeeding in Daily Fantasy Sports.  If no skill is required and success is only a matter of luck, these games could be considered gambling and would be subject to the rules and regulations of each state.

Leveraging a dataset of NBA players past game performances, we plan to develop a recommendation system that provides an end-user with the optimal team for any given day.  If the recommended team performs well enough that the end-user is successful then we will have shown that Daily Fantasy is not a game predicated on luck.

### Project Structure

```
*
* scripts/
	* data-scraper/ - python script we built to collect data-scraper
	* recommend.py - reads clusters and aggregates data in order to return an optimal line-up
	* tiers.py - aggregates data and creates clusters to be used later
* clustersGroupsed.csv - tiers.py output store
* DailyFantasyRecommender.sh - Executable to run the project
* DKSalaries.vsc - Example daily lineup from DraftKings
* raw-data.zip - Repository tracking of the dataset
*
```

## Getting started

1. Unpack *raw-data.zip* into **scripts/data-scraper/data**
2.	Make sure *DailyFantasyRecommender.sh* has executable permissions `chmod +x DailyFantasyRecommender.sh`
3. Execute *DailyFantasyRecommender.sh* with path to Apache Spark `./DailyFantasyRecommender.sh ../spark`

## Errata
