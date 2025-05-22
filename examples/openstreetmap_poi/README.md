# Quickstart with Docker Compose

This guide shows how to quickly set up the OSM POI example using Docker Compose and PostGIS.

## 1. Start the PostGIS Database

From the `examples/openstreetmap_poi/` directory, run:

```bash
docker-compose up -d
```

This will start a PostgreSQL database with PostGIS enabled, accessible on port 55432.

## 2. Configure Environment Variables

Copy the template and edit as needed:

```bash
cp .env.example .env
```

Edit `.env` and set:
- `DB_HOST=db`
- `DB_PORT=55432`
- `DB_USER=osmuser`
- `DB_PASSWORD=osmpassword`
- `DB_NAME=osm_poi_db`

These match the defaults in `db.env` and `docker-compose.yml`.

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
