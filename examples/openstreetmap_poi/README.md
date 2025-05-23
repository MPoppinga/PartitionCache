# Quickstart with Docker Compose

This guide shows how to quickly set up the OSM POI example using Docker Compose and PostGIS.

## 1. Start the PostGIS Database

From the `examples/openstreetmap_poi/` directory, run:

```bash
docker-compose up -d
```

This will start a PostgreSQL database with PostGIS enabled, accessible on port 55432.

## 2. Configure Environment Variables

You can use the provided `.env.example` file to configure the environment variables.



## 3. Install Python Dependencies

From the project root:

```bash
pip install -r requirements.txt
```

## 4. Import OSM Data

Run the import script (this may take a while for large OSM files):

```bash
python process_osm_data.py
```

## 5. Run Example Queries

Use the provided script to run example queries, with and without PartitionCache:

```bash
python run_poi_queries.py
```

## 6.a Add a query to the cache

```bash
pcache-add --direct --query-file examples/openstreetmap_poi/testqueries_examples/q1.sql --partition-key zipcode --env examples/openstreetmap_poi/.env.example 
```

## 6.b Monitor the cache queue

```bash
pcache-monitor --env examples/openstreetmap_poi/.env.example
```

Add query to the cache queue

```bash
pcache-add --query-file examples/openstreetmap_poi/testqueries_examples/q1.sql --partition-key zipcode --env examples/openstreetmap_poi/.env.example --queue-provider postgresql 
# or
pcache-add --queue --query-file examples/openstreetmap_poi/testqueries_examples/q1.sql --partition-key zipcode --env examples/openstreetmap_poi/.env.example --queue-original
```


