#!/usr/bin/env python3
"""
Wikipedia Data Generator for PartitionCache LLM Benchmark.

Downloads Simple English Wikipedia XML dumps, parses articles with real metadata,
and loads them into PostgreSQL (timescaledb-ha with pgai extension).

Data sources (from https://dumps.wikimedia.org/simplewiki/latest/):
    - simplewiki-latest-pages-articles-multistream.xml.bz2 (~200MB) - article content
    - simplewiki-latest-stub-meta-history.xml.gz (~varies) - revision metadata

Tables:
    - wikipedia_articles: ~280K articles with real metadata

Usage:
    # Full load with mock LLM function (no Ollama needed)
    python generate_wikipedia_data.py --mock

    # Limited load for testing
    python generate_wikipedia_data.py --max-articles 1000 --mock

    # Full load with real LLM function (requires Ollama + pgai)
    python generate_wikipedia_data.py

    # Skip download if dumps already exist
    python generate_wikipedia_data.py --skip-download --mock
"""

import argparse
import bz2
import csv
import gzip
import os
import sys
import tempfile
import urllib.request
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Wikipedia dump URLs
DUMP_BASE_URL = "https://dumps.wikimedia.org/simplewiki/latest"
ARTICLES_DUMP = "simplewiki-latest-pages-articles-multistream.xml.bz2"
STUB_HISTORY_DUMP = "simplewiki-latest-stub-meta-history.xml.gz"

# Schema DDL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS wikipedia_articles (
    article_id    INTEGER PRIMARY KEY,
    title         VARCHAR(512) NOT NULL,
    content       TEXT,
    content_length INTEGER,
    creation_date TIMESTAMP,
    creation_year INTEGER,
    last_modified TIMESTAMP,
    edit_count    INTEGER,
    categories    TEXT[],
    num_categories INTEGER,
    num_references INTEGER,
    num_wikilinks  INTEGER,
    infobox_type  VARCHAR(128)
);
"""

CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_wa_creation_year ON wikipedia_articles(creation_year);
CREATE INDEX IF NOT EXISTS idx_wa_creation_date ON wikipedia_articles(creation_date);
CREATE INDEX IF NOT EXISTS idx_wa_edit_count ON wikipedia_articles(edit_count);
CREATE INDEX IF NOT EXISTS idx_wa_content_length ON wikipedia_articles(content_length);
CREATE INDEX IF NOT EXISTS idx_wa_infobox_type ON wikipedia_articles(infobox_type);
CREATE INDEX IF NOT EXISTS idx_wa_num_categories ON wikipedia_articles(num_categories);
CREATE INDEX IF NOT EXISTS idx_wa_num_references ON wikipedia_articles(num_references);
CREATE INDEX IF NOT EXISTS idx_wa_categories ON wikipedia_articles USING GIN (categories);
"""


def download_file(url: str, dest_path: str) -> None:
    """Download a file with progress reporting."""
    print(f"  Downloading {url}")
    try:
        from tqdm import tqdm

        response = urllib.request.urlopen(url)  # noqa: S310
        total_size = int(response.headers.get("Content-Length", 0))

        with open(dest_path, "wb") as f, tqdm(total=total_size, unit="B", unit_scale=True, desc=Path(dest_path).name) as pbar:
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                f.write(chunk)
                pbar.update(len(chunk))
    except ImportError:
        # Fallback without tqdm
        print("  (install tqdm for progress bars)")
        response = urllib.request.urlopen(url)  # noqa: S310
        total = 0
        with open(dest_path, "wb") as f:
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                f.write(chunk)
                total += len(chunk)
                if total % (10 * 1024 * 1024) == 0:
                    print(f"    {total / (1024 * 1024):.0f} MB downloaded...")
        print(f"  Downloaded {total / (1024 * 1024):.1f} MB")


def parse_stub_history(stub_path: str) -> dict:
    """Parse stub-meta-history dump to extract edit counts and creation dates.

    Returns dict: page_id -> {edit_count, creation_date}
    """
    import mwxml

    print("Phase 2: Parsing stub-meta-history for edit counts and creation dates...")
    page_metadata: dict[int, dict] = {}
    article_count = 0

    # Open gzipped file
    with gzip.open(stub_path, "rb") as f:
        dump = mwxml.Dump.from_file(f)
        for page in dump:
            if page.namespace != 0:  # articles only
                continue

            revision_count = 0
            first_timestamp = None
            for revision in page:
                revision_count += 1
                if first_timestamp is None:
                    first_timestamp = revision.timestamp

            page_metadata[page.id] = {
                "edit_count": revision_count,
                "creation_date": first_timestamp,
            }
            article_count += 1
            if article_count % 50000 == 0:
                print(f"    Processed {article_count} article histories...")

    print(f"  Found metadata for {len(page_metadata)} articles")
    return page_metadata


def parse_articles(articles_path: str, page_metadata: dict, max_articles: int | None = None) -> list[dict]:
    """Parse pages-articles dump to extract content and join with metadata.

    Returns list of article dicts ready for database insertion.
    """
    import mwparserfromhell
    import mwxml

    print("Phase 3: Parsing article content...")
    articles = []
    skipped = 0

    with bz2.open(articles_path, "rb") as f:
        dump = mwxml.Dump.from_file(f)
        for page in dump:
            if page.namespace != 0:  # articles only
                continue

            if max_articles and len(articles) >= max_articles:
                break

            for revision in page:  # only latest revision in pages-articles dump
                text = revision.text
                if not text or len(text.strip()) < 50:
                    skipped += 1
                    continue

                try:
                    wikicode = mwparserfromhell.parse(text)
                except Exception:
                    skipped += 1
                    continue

                # Extract plain text (strip markup)
                plain_text = wikicode.strip_code()
                if len(plain_text.strip()) < 50:
                    skipped += 1
                    continue

                # Extract categories: [[Category:Name]]
                categories = []
                non_category_wikilinks = 0
                for link in wikicode.filter_wikilinks():
                    title_str = str(link.title)
                    if title_str.startswith("Category:"):
                        categories.append(title_str[len("Category:") :])
                    else:
                        non_category_wikilinks += 1

                # Extract infobox type: {{Infobox person ...}}
                infobox_type = None
                for template in wikicode.filter_templates():
                    name = str(template.name).strip()
                    if name.lower().startswith("infobox"):
                        infobox_type = name[len("infobox") :].strip()
                        if infobox_type:
                            # Truncate long infobox types
                            infobox_type = infobox_type[:128]
                        else:
                            infobox_type = None
                        break

                # Count references
                num_references = 0
                try:
                    for tag in wikicode.filter_tags():
                        if hasattr(tag, "tag") and str(tag.tag) == "ref":
                            num_references += 1
                except Exception:
                    # Some malformed tags can cause issues
                    num_references = text.lower().count("<ref")

                # Join with stub metadata
                meta = page_metadata.get(page.id, {})
                creation_date = meta.get("creation_date")

                article = {
                    "article_id": page.id,
                    "title": page.title[:512] if page.title else "",
                    "content": plain_text,
                    "content_length": len(plain_text),
                    "creation_date": creation_date,
                    "creation_year": int(creation_date.strftime("%Y")) if creation_date else None,
                    "last_modified": revision.timestamp,
                    "edit_count": meta.get("edit_count", 0),
                    "categories": categories,
                    "num_categories": len(categories),
                    "num_references": num_references,
                    "num_wikilinks": non_category_wikilinks,
                    "infobox_type": infobox_type,
                }
                articles.append(article)

                if len(articles) % 10000 == 0:
                    print(f"    Parsed {len(articles)} articles...")

    print(f"  Parsed {len(articles)} articles ({skipped} skipped)")
    return articles


def format_pg_array(items: list[str]) -> str:
    """Format a Python list as a PostgreSQL TEXT[] literal for CSV."""
    if not items:
        return "{}"
    # Escape quotes and backslashes within elements
    escaped = []
    for item in items:
        item = item.replace("\\", "\\\\").replace('"', '\\"')
        escaped.append(f'"{item}"')
    return "{" + ",".join(escaped) + "}"


def format_timestamp(ts) -> str:
    """Format a timestamp for CSV output."""
    if ts is None:
        return ""
    if hasattr(ts, "strftime"):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)


def load_into_postgresql(articles: list[dict], conn, mock: bool) -> None:
    """Load articles into PostgreSQL via COPY."""
    print(f"Phase 4: Loading {len(articles)} articles into PostgreSQL...")

    with conn.cursor() as cur:
        # Drop and recreate table
        cur.execute("DROP TABLE IF EXISTS wikipedia_articles CASCADE")
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()

        # Write to temp CSV and COPY
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8") as tmp:
            tmp_path = tmp.name
            writer = csv.writer(tmp, quoting=csv.QUOTE_MINIMAL)
            writer.writerow([
                "article_id",
                "title",
                "content",
                "content_length",
                "creation_date",
                "creation_year",
                "last_modified",
                "edit_count",
                "categories",
                "num_categories",
                "num_references",
                "num_wikilinks",
                "infobox_type",
            ])
            for a in articles:
                writer.writerow([
                    a["article_id"],
                    a["title"],
                    a["content"],
                    a["content_length"],
                    format_timestamp(a["creation_date"]),
                    a["creation_year"] if a["creation_year"] is not None else "",
                    format_timestamp(a["last_modified"]),
                    a["edit_count"],
                    format_pg_array(a["categories"]),
                    a["num_categories"],
                    a["num_references"],
                    a["num_wikilinks"],
                    a["infobox_type"] if a["infobox_type"] else "",
                ])

        try:
            with open(tmp_path, "rb") as f:
                with cur.copy("COPY wikipedia_articles FROM STDIN WITH (FORMAT CSV, HEADER, NULL '')") as copy:
                    while True:
                        data = f.read(65536)
                        if not data:
                            break
                        copy.write(data)
            conn.commit()
            print(f"  Loaded {len(articles)} articles via COPY")
        finally:
            os.unlink(tmp_path)

        # Create indexes
        print("  Creating indexes...")
        for stmt in CREATE_INDEXES_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()

        # Analyze table
        cur.execute("ANALYZE wikipedia_articles")
        conn.commit()

    # Install LLM functions
    install_llm_functions(conn, mock=mock)


def install_llm_functions(conn, mock: bool = False) -> None:
    """Install LLM classifier functions from setup_llm_functions.sql."""
    sql_path = os.path.join(os.path.dirname(__file__), "setup_llm_functions.sql")
    with open(sql_path) as f:
        sql = f.read()

    with conn.cursor() as cur:
        # Install all functions from the SQL file
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                # Need to handle $$ delimited functions - execute full file instead
                break
        else:
            return

        # Execute the full SQL file as-is (handles $$ delimiters)
        cur.execute(sql)
        conn.commit()
        print("  Installed LLM functions (real + mock)")

        if mock:
            # Replace real function with mock for testing
            cur.execute("""
                CREATE OR REPLACE FUNCTION wiki_llm_classify(
                    content TEXT,
                    question TEXT,
                    model_name TEXT DEFAULT 'mock',
                    max_content_length INTEGER DEFAULT 2000
                ) RETURNS BOOLEAN AS $$
                BEGIN
                    RETURN (abs(hashtext(LEFT(content, max_content_length) || question)) % 10) < 3;
                END;
                $$ LANGUAGE plpgsql;
            """)
            conn.commit()
            print("  Replaced wiki_llm_classify with mock (deterministic, ~30% TRUE rate)")


def install_pgai_extension(conn) -> None:
    """Try to install pgai extension. Non-fatal if not available."""
    with conn.cursor() as cur:
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS ai CASCADE")
            conn.commit()
            print("  pgai extension installed")
        except Exception as e:
            conn.rollback()
            print(f"  WARNING: Could not install pgai extension: {e}")
            print("  LLM functions will only work in mock mode")


def print_summary(conn) -> None:
    """Print dataset summary statistics."""
    print("\n" + "=" * 60)
    print("Dataset Summary")
    print("=" * 60)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MIN(article_id), MAX(article_id) FROM wikipedia_articles")
        count, min_id, max_id = cur.fetchone()
        print(f"  Total articles: {count:,}")
        print(f"  Article ID range: {min_id:,} - {max_id:,}")

        cur.execute("""
            SELECT
                MIN(creation_year) AS min_year,
                MAX(creation_year) AS max_year,
                AVG(edit_count)::INTEGER AS avg_edits,
                MAX(edit_count) AS max_edits,
                AVG(content_length)::INTEGER AS avg_length,
                AVG(num_categories)::INTEGER AS avg_cats,
                COUNT(*) FILTER (WHERE infobox_type IS NOT NULL) AS with_infobox,
                AVG(num_references)::INTEGER AS avg_refs,
                AVG(num_wikilinks)::INTEGER AS avg_links
            FROM wikipedia_articles
        """)
        row = cur.fetchone()
        print(f"  Creation year range: {row[0]} - {row[1]}")
        print(f"  Avg edit count: {row[2]:,} (max: {row[3]:,})")
        print(f"  Avg content length: {row[4]:,} chars")
        print(f"  Avg categories: {row[5]}")
        print(f"  Articles with infobox: {row[6]:,}")
        print(f"  Avg references: {row[7]}")
        print(f"  Avg wikilinks: {row[8]}")

        # Top categories (unnest array)
        print("\n  Top 10 categories:")
        cur.execute("""
            SELECT c, COUNT(*) AS cnt
            FROM wikipedia_articles, unnest(categories) AS c
            GROUP BY c ORDER BY cnt DESC LIMIT 10
        """)
        for cat, cnt in cur.fetchall():
            print(f"    {cat}: {cnt:,}")

        # Top infobox types
        print("\n  Top 10 infobox types:")
        cur.execute("""
            SELECT infobox_type, COUNT(*) AS cnt
            FROM wikipedia_articles
            WHERE infobox_type IS NOT NULL
            GROUP BY infobox_type ORDER BY cnt DESC LIMIT 10
        """)
        for itype, cnt in cur.fetchall():
            print(f"    {itype}: {cnt:,}")

        # Selectivity estimates for benchmark queries
        print("\n  Query selectivity estimates:")
        selectivity_queries = [
            ("History category + edit>50 + year>=2005 (Flight 1)", """
                SELECT COUNT(*) FILTER (
                    WHERE EXISTS (SELECT 1 FROM unnest(categories) c WHERE c ILIKE '%%histor%%')
                      AND edit_count > 50 AND creation_year >= 2005
                ) * 100.0 / COUNT(*) FROM wikipedia_articles
            """),
            ("Year 2010-2015 + length>5000 (Flight 2)", """
                SELECT COUNT(*) FILTER (
                    WHERE creation_year BETWEEN 2010 AND 2015 AND content_length > 5000
                ) * 100.0 / COUNT(*) FROM wikipedia_articles
            """),
            ("Science cats + edit>100 + refs>20 (Flight 3)", """
                SELECT COUNT(*) FILTER (
                    WHERE EXISTS (SELECT 1 FROM unnest(categories) c
                                  WHERE c ILIKE '%%scien%%' OR c ILIKE '%%physic%%'
                                     OR c ILIKE '%%chemi%%' OR c ILIKE '%%biolog%%')
                      AND edit_count > 100 AND num_references > 20
                ) * 100.0 / COUNT(*) FROM wikipedia_articles
            """),
            ("Multi-constraint (Flight 4)", """
                SELECT COUNT(*) FILTER (
                    WHERE EXISTS (SELECT 1 FROM unnest(categories) c WHERE c ILIKE '%%histor%%')
                      AND creation_year >= 2008
                      AND content_length BETWEEN 3000 AND 50000
                      AND edit_count > 30
                      AND infobox_type IS NOT NULL
                ) * 100.0 / COUNT(*) FROM wikipedia_articles
            """),
        ]
        for label, query in selectivity_queries:
            try:
                cur.execute(query)
                pct = cur.fetchone()[0]
                if pct is not None:
                    print(f"    {label}: {pct:.2f}%")
                else:
                    print(f"    {label}: 0.00%")
            except Exception as e:
                print(f"    {label}: error ({e})")


def main():
    parser = argparse.ArgumentParser(description="Generate Wikipedia benchmark data for PartitionCache LLM benchmark")
    parser.add_argument(
        "--max-articles",
        type=int,
        default=None,
        help="Maximum number of articles to load (default: all ~280K)",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Install mock LLM classifier (no Ollama needed)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download, use existing dump files",
    )
    parser.add_argument(
        "--dump-dir",
        type=str,
        default=None,
        help="Directory for dump files (default: ./data/)",
    )
    parser.add_argument("--db-name", type=str, default=None, help="PostgreSQL database name")
    parser.add_argument("--db-host", type=str, default=None, help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=None, help="PostgreSQL port")
    parser.add_argument("--db-user", type=str, default=None, help="PostgreSQL user")
    parser.add_argument("--db-password", type=str, default=None, help="PostgreSQL password")

    args = parser.parse_args()

    try:
        import psycopg
        from dotenv import load_dotenv
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install psycopg[binary] python-dotenv mwxml mwparserfromhell tqdm")
        sys.exit(1)

    try:
        import mwparserfromhell  # noqa: F401
        import mwxml  # noqa: F401
    except ImportError as e:
        print(f"Missing Wikipedia parsing dependency: {e}")
        print("Install with: pip install mwxml mwparserfromhell")
        sys.exit(1)

    # Load .env configuration
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

    db_host = args.db_host or os.getenv("DB_HOST", "localhost")
    db_port = args.db_port or int(os.getenv("DB_PORT", "5435"))
    db_user = args.db_user or os.getenv("DB_USER", "app_user")
    db_password = args.db_password or os.getenv("DB_PASSWORD", "")
    db_name = args.db_name or os.getenv("DB_NAME", "wikipedia_db")

    dump_dir = args.dump_dir or os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(dump_dir, exist_ok=True)

    articles_path = os.path.join(dump_dir, ARTICLES_DUMP)
    stub_path = os.path.join(dump_dir, STUB_HISTORY_DUMP)

    # Phase 1: Download dumps
    if not args.skip_download:
        print("Phase 1: Downloading Wikipedia dumps...")
        for filename, filepath in [(ARTICLES_DUMP, articles_path), (STUB_HISTORY_DUMP, stub_path)]:
            if os.path.exists(filepath):
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                print(f"  {filename} already exists ({size_mb:.1f} MB), skipping")
            else:
                url = f"{DUMP_BASE_URL}/{filename}"
                download_file(url, filepath)
    else:
        print("Phase 1: Skipping download (--skip-download)")
        for filepath in [articles_path, stub_path]:
            if not os.path.exists(filepath):
                print(f"  ERROR: {filepath} not found")
                sys.exit(1)

    # Phase 2: Parse stub history
    page_metadata = parse_stub_history(stub_path)

    # Phase 3: Parse articles
    articles = parse_articles(articles_path, page_metadata, max_articles=args.max_articles)

    if not articles:
        print("ERROR: No articles parsed. Check dump files.")
        sys.exit(1)

    # Phase 4: Load into PostgreSQL
    print(f"\nConnecting to PostgreSQL at {db_host}:{db_port}/{db_name}...")
    conn = psycopg.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname=db_name,
        autocommit=False,
    )

    try:
        # Try to install pgai extension (non-fatal)
        if not args.mock:
            install_pgai_extension(conn)

        load_into_postgresql(articles, conn, mock=args.mock)
        print_summary(conn)
    finally:
        conn.close()

    print("\nDone!")
    if args.mock:
        print("NOTE: Using mock LLM classifier. For real LLM, run without --mock and ensure Ollama is running.")
    else:
        print("NOTE: Real LLM classifier installed. Ensure Ollama is running with a pulled model:")
        print("  docker exec <ollama-container> ollama pull qwen3.5:4b")


if __name__ == "__main__":
    main()
