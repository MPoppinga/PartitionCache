# PartitionCache

## Objective
A caching middelware allowing to cache partition indentifierts in heavily partitioned datasets.
This middelware allows executing kown queries and storing a set of partitions with valid results.
For following queries this entries can be used and combined to reduce the search space.

Currently PostgreSQL can be used as underling database, while various choices are avaliable as cache backend.
It can be used as CLI or as python library

## Presumptions

To efficently utilize this approach the problem needs to fulfill the following properties:
- Complex SQL queries which need more than a few milliseconds to compute
- Readonly dataset (Appendable datasetas are WIP)
- (Analytical) Searches across seperate partitions (e.g. Cities, spacial areas, Days for time series or sets of connected networks of property graphs)
    - This will work only partially with aggegations across searchspaces
- For positive effects on new queries: Different subqueries need to be conjunctive (AND) connected




## How does it work?

Elimination of search space

- Incomming queries get recomposed and many variants are executed against the database
- All recomposed queries get stored together with a set of partition identifiers, showing in which partitions results were found
- Frequent subqueries get also stored with their partition indetifiers
- New queries get checked against the cache. If the query or a part of the query is present in the cache the search space can be restricted.
- On append only datasets we can rule out specific partitions for known (subqueries) so the search needs only be performed on not excluded as well as new search spaces.



###  Example

"For all Cities in the world find where a public park larger than 1000 m² lies within 50m of a street named "main street"."

For this type of search we can seperate the search space for each city in our dataset. On a normal search this would not help much, but with PartitionCache we can store the cities for which these search was sucessfull.
If we have now another search which uses the same query, for example; "For all Cities in Europe find where a public park larger than 1000 m² lies within 50m of a street named "main street"." We do not need to recompute this task for all cities, we can take the list of cities from the earlier search and only check fo them the additional contraints, and with clasical database optimization this can be done very fast.

Further, if we observerve tha many requests are containing the condition "a public park larger than 1000 m²" We can create a list of cities where the parc exists and also speedup queries which ask for other properies. 
Like "all schools in europe which are within 1km of a public park larger than 1000 m²", in this case we can intersect the List of cities in Europe, with the List of Cities with a park and need to look for schools only in these cities.



## Install

pip install git@... #TODO


##  Usage

Notes:
At the current state the library does only support a specific subset of SQL syntax which is avaliable for PostgreSQL.
For Example CTEs are not supported and JOINS and Subqueries are only partially suported. We aim to increase the robustnes and lexibility of our approach in future releases.

This tool is used to improve query execution times for tools like GeoMine, PyGeoMine and ComplexMine


### CLI Usage

####  Cache population
Usage: pcache-observer # TODO document

pcache-observer is a process, thath monitores the queue for new cach requests, allowing asyncronous population of the cache

Usage: pcache-add # TODO document

pcache-add allows to add individual queries to the cache directly or send them to the queue

```
pcache-add \
  --query "SELECT * FROM table" \
  --db-name mydb \
  --env-file .env \
  --cache-backend redis

```


```
pcache-add \
  --query "SELECT * FROM table" \
  --queue \
  --env-file .env
```

#### Cache Usage

Usage: pcache-get # TODO document

pcache-get returns the list of all partition keys based on the provided query, restricting the search space to as few partition_keys as possible

```
pcache-get \
  --query "SELECT * FROM table" \
  --cache-backend rocksdb

```


#### Cache Management

pcache-manage allows to copy, delete, archive entries from the individial cache backends thath have been set up

Usage: pcache-manage # TODO document

### Direct Usage
If using a python application its possible to direclty call the relevant functions. # TODO comprehensive documenttion


