import osmium
import psycopg2
import requests
from shapely.geometry import Point, Polygon, MultiPolygon
from rtree import index
import time


from dotenv import load_dotenv
import os
from tqdm import tqdm  # Added tqdm for improved progress bars

# Load environment variables from .env file
load_dotenv(".env", override=True)


# Database connection parameters
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
 
# OSM file path

OSM_FILE = os.getenv("OSM_FILE", "germany-latest.osm.pbf")
OSM_URL = os.getenv("OSM_URL", "https://download.geofabrik.de/europe/germany-latest.osm.pbf")


class POIHandler(osmium.SimpleHandler):
    def __init__(self):
        super(POIHandler, self).__init__()
        self.pois = []
        self.postal_areas = index.Index()
        self.postal_codes = {}
        self.progress_bar = tqdm(desc="Processing POIs", unit="elements", dynamic_ncols=True)

    def area(self, a):
        if 'postal_code' in a.tags:
            try:
                outer_rings = []
                inner_rings = []
                for outer_ring in a.outer_rings():
                    outer_rings.append([(n.lon, n.lat) for n in outer_ring])
                    for inner_ring in a.inner_rings(outer_ring):
                        inner_rings.append([(n.lon, n.lat) for n in inner_ring])
                
                if outer_rings:
                    if len(outer_rings) == 1:
                        polygon = Polygon(outer_rings[0], inner_rings)
                    else:
                        polygon = MultiPolygon([Polygon(ring, inner_rings) for ring in outer_rings])
                
                    self.postal_areas.insert(a.id, polygon.bounds, obj=(polygon, a.tags['postal_code']))
                else:
                    print(f"Warning: No outer rings found for area {a.id}")
            except Exception as e:
                print(f"Error processing area {a.id}: {str(e)}")

        # get centroid for pois (which are areas)
        # check if ['amenity', 'shop', 'tourism', 'leisure']: is in tags
        if any(key in a.tags for key in ['amenity', 'shop', 'tourism', 'leisure']):
            for outer_ring in a.outer_rings():
                polygon = Polygon([(n.lon, n.lat) for n in outer_ring])
                centroid = polygon.centroid
                tags = a.tags
                poi_type, poi_subtype = self.get_poi_type(tags)
                self.pois.append({
                    'id': a.id,
                    'name': tags.get('name', ''),
                    'type': poi_type,
                    'subtype': poi_subtype,
                    'zipcode': a.tags.get('postal_code', None),
                    'geom': centroid
                })
        self.print_progress()

    def node(self, n):
        tags = n.tags
        poi_type, poi_subtype = self.get_poi_type(tags)
        if poi_type:
            point = Point(n.location.lon, n.location.lat)
            zipcode = None  # Must be to later as areas not available: self.get_zipcode(point)
            self.pois.append({
                'id': n.id,
                'name': tags.get('name', ''),
                'type': poi_type,
                'subtype': poi_subtype,
                'zipcode': zipcode,
                'geom': point
            })
        self.print_progress()

    def get_poi_type(self, tags):
        for key in ['amenity', 'shop', 'tourism', 'leisure']:
            if key in tags:
                return key, tags[key]
        return None, None

    def get_zipcode(self, point):
        for item in self.postal_areas.intersection(point.bounds, objects=True):
            if item.object is None:
                continue
            area, zipcode = item.object
            if point.within(area):
                try:
                    return int(zipcode)
                except ValueError:
                    return None
        return None

    def print_progress(self):
        self.progress_bar.update(1)

    def close_progress(self):
        self.progress_bar.close()

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS pois;
        CREATE TABLE IF NOT EXISTS pois (
            id TEXT PRIMARY KEY,
            name TEXT,
            type TEXT,
            subtype TEXT,
            zipcode INTEGER,
            geom GEOMETRY(Point, 25832)
        );

        
        """)
    conn.commit()
    print("Table and indexes created successfully")

def insert_pois(conn, pois):
    start_time = time.time()
    total = len(pois)

    with tqdm(total=total, desc="Inserting POIs", unit="POI", dynamic_ncols=True) as pbar:
        with conn.cursor() as cur:
            for i, poi in enumerate(pois, 1):
                cur.execute("""
                INSERT INTO pois (id, name, type, subtype, zipcode, geom)
                VALUES (%s, %s, %s, %s, %s, ST_Transform(ST_SetSRID(ST_GeomFromText(%s), 4326), 25832))
                ON CONFLICT (id) DO NOTHING;
                """, (poi['id'], poi['name'], poi['type'], poi['subtype'], poi['zipcode'], poi['geom'].wkt))
                pbar.update(1)
                if i % 1000 == 0:
                    conn.commit()
    conn.commit()
    elapsed_time = time.time() - start_time
    print(f"\nInserted all {total} POIs successfully in {elapsed_time:.2f} seconds")

def main():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Database connection successful")
        
        # setup postgis if not already done
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            conn.commit()            
        
        cur.close()
        conn.close()
        
    except psycopg2.OperationalError as e:
        print(f"Database connection failed: {e}")
        return
    
    if not os.path.exists(OSM_FILE):
        # Download the OSM file
        print(f"Downloading OSM file from {OSM_URL}...")
        response = requests.get(OSM_URL)
        with open(OSM_FILE, 'wb') as f:
            f.write(response.content)
        print(f"OSM file downloaded and saved as {OSM_FILE}")
        
    print("Starting POI extraction from OSM file...")
    handler = POIHandler()
    handler.apply_file(OSM_FILE)

    # set the pois to the postal codes
    for poi in handler.pois:
        poi["zipcode"] = handler.get_zipcode(poi["geom"])   

    ## pickle the pois
    #with open('pois.pkl', 'wb') as f:
    #    pickle.dump(handler.pois, f)

    handler.close_progress()

    print(f"\nExtracted {len(handler.pois)} POIs from OSM file")

    print("Connecting to the database...")
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    print("Creating table and indexes...")
    create_table(conn)

    print("Inserting POIs into the database...")
    insert_pois(conn, handler.pois)
    
    
    sql = """CREATE INDEX IF NOT EXISTS pois_geom_idx ON pois USING GIST (geom);
        CREATE INDEX IF NOT EXISTS pois_type_idx ON pois (type);
        CREATE INDEX IF NOT EXISTS pois_subtype_idx ON pois (subtype);
        CREATE INDEX IF NOT EXISTS pois_idx ON pois (zipcode, type, subtype, geom);
        CREATE MATERIALIZED VIEW IF NOT EXISTS zipcodes AS 
        SELECT DISTINCT zipcode FROM pois;
        REFRESH MATERIALIZED VIEW zipcodes;
        """
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    
    with conn.cursor() as cur:
        cur.execute("CLUSTER pois USING pois_idx;")
        conn.commit()

    conn.close()
    print("Database connection closed")
    print("POI extraction and import completed successfully")

if __name__ == "__main__":
    main()