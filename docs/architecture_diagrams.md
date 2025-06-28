# PartitionCache Architecture Diagrams

This document contains detailed architectural diagrams for PartitionCache components and data flows.

## System Architecture Overview

```mermaid
graph TB
    %% User Interface Layer
    User[User Application]
    CLI[CLI Tools]
    
    %% Core API Layer
    API[PartitionCache API<br/>create_cache_helper<br/>get_partition_keys<br/>extend_query_with_partition_keys]
    
    %% Query Processing
    QP[Query Processor<br/>Fragment Generation<br/>Query Normalization<br/>Hash Generation]
    
    %% Queue System
    subgraph Queue["Queue System"]
        OQ[Original Query Queue]
        FQ[Fragment Query Queue]
        QH[Queue Handlers<br/>PostgreSQL/Redis]
    end
    
    %% Cache System
    subgraph Cache["Cache System"]
        CH[Cache Helper<br/>Partition-specific wrapper]
        CHL[Cache Handler Layer]
        
        subgraph Handlers["Cache Handlers"]
            PG_A[PostgreSQL Array]
            PG_B[PostgreSQL Bit]
            PG_R[PostgreSQL Roaring]
            RED_S[Redis Set]
            RED_B[Redis Bit]
            ROCK_S[RocksDB Set]
            ROCK_B[RocksDB Bit]
            ROCK_D[RocksDict]
        end
    end
    
    %% Database Layer
    subgraph Database["Database Layer"]
        DBH[DB Handler Layer]
        PG_DB[(PostgreSQL)]
        MY_DB[(MySQL)]
        SQ_DB[(SQLite)]
    end
    
    %% Processing Components
    Monitor[Queue Monitor<br/>Background Processor]
    DirectProc[PostgreSQL Queue Processor<br/>pg_cron integration]
    
    %% CLI Components
    subgraph CLI_Tools["CLI Tools"]
        ManageCache[Cache Management]
        MonitorQueue[Queue Monitoring]
        AddCache[Add to Cache]
        ReadCache[Read from Cache]
        SetupPostgreSQLQueue[Setup PostgreSQL Queue Processor]
    end
    
    %% Flow connections
    User -->|SQL Query| API
    CLI --> CLI_Tools
    CLI_Tools --> API
    CLI_Tools --> Queue
    CLI_Tools --> Cache
    
    API -->|Process Query| QP
    QP -->|Generate Fragments| Queue
    API -->|Check Cache| Cache
    
    Queue --> Monitor
    Queue --> DirectProc
    Monitor -->|Execute Fragments| DBH
    DirectProc -->|Execute Fragments| PG_DB
    
    Monitor -->|Store Results| Cache
    DirectProc -->|Store Results| Cache
    
    API -->|Wrap Handler| CH
    CH -->|Delegate Operations| CHL
    CHL --> Handlers
    
    DBH --> PG_DB
    DBH --> MY_DB
    DBH --> SQ_DB
    
    %% Cache to Database connections
    PG_A -.->|Metadata Tables| PG_DB
    PG_B -.->|Metadata Tables| PG_DB
    PG_R -.->|Metadata Tables| PG_DB
    RED_S -.->|Cache Storage| Redis[(Redis)]
    RED_B -.->|Cache Storage| Redis
    ROCK_S -.->|Cache Storage| RocksDB[(RocksDB)]
    ROCK_B -.->|Cache Storage| RocksDB
    ROCK_D -.->|Cache Storage| RocksDB
    
    %% Queue to Database connections  
    QH -.->|Queue Tables| PG_DB
    QH -.->|Queue Lists| Redis
    
    %% Data flow annotations
    OQ -->|Fragment Generation| FQ
    FQ -->|Process Items| Monitor
    FQ -->|Process Items| DirectProc
    
    %% Return path
    Cache -->|Partition Keys| API
    API -->|Extended Query| User
    
    style API fill:#e1f5fe
    style Cache fill:#f3e5f5
    style Queue fill:#e8f5e8
    style Database fill:#fff3e0
    style CLI_Tools fill:#fce4ec
```

## Query Processing Flow

```mermaid
flowchart TD
    Start([User Submits Query])
    
    %% PartitionCache Library/CLI Processing
    subgraph PCLib["PartitionCache Library / CLI"]
        Fragment[Query Gets<br/>Fragmented]
        Variants[Variants Get Created]
        Hash[Stable Representation<br/>and Hashing]
    end
    
    %% Cache Check Phase
    Check{Check Cache for<br/>Partition Keys}
    Cache[(Cache)]
    
    %% Decision Paths
    NoHits[Use Original Query]
    FoundHits[Extend query to be<br/>restricted to<br/>Intersection of Found<br/>Partitions]
    
    %% Queue Processing
    ReducedSet[Reduced Set of Variants]
    Queue[Queue]
    
    %% Background Processing
    subgraph Processing["Background Processing"]
        Pop[Pop Fragment From<br/>Queue]
        Execute[Execute Fragment on<br/>Database]
        Store[Store results]
    end
    
    %% Database
    DB[(DB)]
    
    %% Flow connections
    Start --> Fragment
    Fragment --> Variants
    Variants --> Hash
    Hash --> Check
    Check <--> Cache
    
    Check -->|No Hits| NoHits
    Check -->|Found Hits| FoundHits
    
    Hash -->|Reduced Set of Variants| ReducedSet
    ReducedSet --> Queue
    
    Queue --> Pop
    Pop --> Execute
    Execute --> Store
    Store --> Cache
    Store --> Pop
    
    Execute --> DB
    NoHits --> DB
    FoundHits --> DB
    
    %% Styling
    style Start fill:#e8f5e8
    style PCLib fill:#f0f8ff
    style Check fill:#fff3e0
    style NoHits fill:#ffe6e6
    style FoundHits fill:#e1f5fe
    style Processing fill:#f3e5f5
    style Cache fill:#e8f5e8
    style DB fill:#fff3e0
```

## PostgreSQL Queue Processor Architecture

```mermaid
graph TB
    subgraph "PostgreSQL Database"
        subgraph "pg_cron Scheduler"
            Cron1[Worker Job 1<br/>Every N seconds]
            Cron2[Worker Job 2<br/>Every N seconds]
            Cron3[Worker Job N<br/>Every N seconds]
            CronTO[Timeout Job<br/>Every N seconds]
        end
        
        subgraph "Queue Tables"
            OrigQueue[original_query_queue<br/>- id, query<br/>- partition_key, priority]
            FragQueue[query_fragment_queue<br/>- id, query, hash<br/>- partition_key, priority]
        end
        
        subgraph "Processing Tables"
            ActiveJobs[active_jobs<br/>- query_hash, partition_key<br/>- job_id, started_at]
            ProcessorLog[processor_log<br/>- job_id, status<br/>- execution_time_ms, error_message]
            Config[processor_config<br/>- enabled, max_parallel_jobs<br/>- frequency_seconds, timeout_seconds]
        end
        
        subgraph "Cache Tables"
            CacheTable[{prefix}_cache_{partition_key}<br/>- query_hash<br/>- value (array/bit/roaring)]
            QueriesTable[{prefix}_queries<br/>- query_hash, query<br/>- partition_key, last_seen]
        end
        
        subgraph "Processing Functions"
            SingleJob[partitioncache_run_single_job]
            Cleanup[partitioncache_handle_timeouts]
            UpdateConfig[partitioncache_update_processor_config]
            SetEnabled[partitioncache_set_processor_enabled]
        end
    end
    
    %% Flow connections
    Cron1 --> SingleJob
    Cron2 --> SingleJob
    Cron3 --> SingleJob
    CronTO --> Cleanup
    
    SingleJob --> OrigQueue
    SingleJob --> FragQueue
    SingleJob --> ActiveJobs
    SingleJob --> ProcessorLog
    SingleJob --> CacheTable
    SingleJob --> QueriesTable
    
    Cleanup --> ActiveJobs
    Cleanup --> ProcessorLog
    
    UpdateConfig --> Config
    SetEnabled --> Config
    
    %% Styling
    style SingleJob fill:#e1f5fe
    style Cleanup fill:#f3e5f5
    style Status fill:#e8f5e8
```

## Cache Handler Class Hierarchy

```mermaid
classDiagram
    class AbstractCacheHandler {
        <<abstract>>
        +set_set(key, value, partition_key="partition_key") bool
        +set_null(key, partition_key="partition_key") bool
        +set_query(key, query, partition_key="partition_key") bool
        +get(key, partition_key="partition_key") set|None
        +exists(key, partition_key="partition_key") bool
        +is_null(key, partition_key="partition_key") bool
        +delete(key, partition_key="partition_key") bool
        +get_all_keys(partition_key="partition_key") list[str]
        +get_partition_keys() list[tuple[str,str]]
        +filter_existing_keys(keys, partition_key) set
        +get_datatype(partition_key) str|None
        +register_partition_key(partition_key, datatype, **kwargs)
        +get_intersected(keys, partition_key) tuple[set,int]
        +close()
    }
    
    class AbstractCacheHandler_Lazy {
        <<abstract>>
        +get_intersected_lazy(keys, partition_key) tuple[str|None,int]
    }
    
    class PostgreSQLArrayCacheHandler {
        +supported_datatypes: set
        -_ensure_partition_table(partition_key, datatype)
        -_get_cache_table_name(partition_key) str
    }
    
    class PostgreSQLBitCacheHandler {
        +supported_datatypes: set
        -_create_partition_table(partition_key, bitsize)
        -_get_partition_bitsize(partition_key) int
    }
    
    class PostgreSQLRoaringBitCacheHandler {
        +supported_datatypes: set
        -_convert_to_roaring(value) BitMap
    }
    
    class RedisSetCacheHandler {
        +supported_datatypes: set
        -_get_redis_key(key, partition_key) str
    }
    
    class RedisBitCacheHandler {
        +supported_datatypes: set
        -_set_bits(key, value, partition_key)
    }
    
    class RocksDBSetCacheHandler {
        +supported_datatypes: set
        -_get_db_path() str
    }
    
    class RocksDBBitCacheHandler {
        +supported_datatypes: set
        -_convert_to_bitarray(value) bitarray
    }
    
    class RocksDictCacheHandler {
        +supported_datatypes: set
        -_serialize_value(value) bytes
        -_deserialize_value(data) set
    }
    
    AbstractCacheHandler <|-- PostgreSQLArrayCacheHandler
    AbstractCacheHandler <|-- PostgreSQLBitCacheHandler
    AbstractCacheHandler <|-- PostgreSQLRoaringBitCacheHandler
    AbstractCacheHandler <|-- RedisSetCacheHandler
    AbstractCacheHandler <|-- RedisBitCacheHandler
    AbstractCacheHandler <|-- RocksDBSetCacheHandler
    AbstractCacheHandler <|-- RocksDBBitCacheHandler
    AbstractCacheHandler <|-- RocksDictCacheHandler
    
    AbstractCacheHandler_Lazy <|-- PostgreSQLArrayCacheHandler
    AbstractCacheHandler_Lazy <|-- PostgreSQLRoaringBitCacheHandler
```

## Queue System Data Flow

```mermaid
sequenceDiagram
    participant App as Application
    participant API as PartitionCache API
    participant OQ as Original Queue
    participant Proc as Queue Processor
    participant FQ as Fragment Queue
    participant Exec as Executor
    participant Cache as Cache Handler
    participant DB as Database
    
    App->>API: Submit Query + Partition Key
    API->>OQ: Push to Original Queue
    
    Note over Proc: Background Processing
    Proc->>OQ: Pop Original Query
    Proc->>Proc: Generate Query Fragments
    Proc->>FQ: Push Fragments to Fragment Queue
    
    Note over Exec: Background Execution
    Exec->>FQ: Pop Fragment Query
    Exec->>DB: Execute Fragment
    DB-->>Exec: Results
    Exec->>Cache: Store Partition Keys
    
    Note over App: Later Query
    App->>API: Same/Similar Query
    API->>Cache: Check for Partition Keys
    Cache-->>API: Return Cached Keys
    API->>API: Optimize Query with Partition Keys
    API-->>App: Return Optimized Query
```

## Multi-Partition Support Architecture

```mermaid
graph LR
    subgraph "Application Queries"
        Q1[Query for Cities]
        Q2[Query for Regions] 
        Q3[Query for Time Buckets]
    end
    
    subgraph "Cache Handler"
        CH[PartitionCacheHelper]
    end
    
    subgraph "Backend Storage"
        subgraph "PostgreSQL"
            PG1[cache_city_id table]
            PG2[cache_region_name table]
            PG3[cache_time_bucket table]
            META[partition_metadata table]
        end
    end
    
    Q1 --> CH
    Q2 --> CH
    Q3 --> CH
    
    CH -->|partition_key=city_id<br/>datatype=integer| PG1
    CH -->|partition_key=region_name<br/>datatype=text| PG2
    CH -->|partition_key=time_bucket<br/>datatype=timestamp| PG3
    
    CH <--> META
    
    META --> |Track datatypes<br/>and partition keys| PG1
    META --> |Track datatypes<br/>and partition keys| PG2
    META --> |Track datatypes<br/>and partition keys| PG3
```

## Cache Eviction System

```mermaid
graph TB
    subgraph "Eviction Triggers"
        Manual[Manual CLI Command]
        Auto[Automatic pg_cron Job]
    end
    
    subgraph "Eviction Strategies"
        Oldest[Oldest Strategy<br/>Based on last_seen]
        Largest[Largest Strategy<br/>Based on entry size]
    end
    
    subgraph "Cache Analysis"
        Scan[Scan Cache Tables]
        Count[Count Entries/Sizes]
        Select[Select Items to Evict]
    end
    
    subgraph "Eviction Actions"
        Delete[Delete Cache Entries]
        Log[Log Eviction Activity]
        Update[Update Statistics]
    end
    
    Manual --> Oldest
    Manual --> Largest
    Auto --> Oldest
    Auto --> Largest
    
    Oldest --> Scan
    Largest --> Scan
    
    Scan --> Count
    Count --> Select
    Select --> Delete
    Delete --> Log
    Log --> Update
```

These diagrams provide visual representation of PartitionCache's architecture, data flows, and component relationships. Each diagram focuses on a specific aspect of the system to aid understanding and implementation.