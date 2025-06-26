# PartitionCache Documentation

This directory contains the technical documentation for PartitionCache, a caching middleware for partition-based query optimization.

## Documentation Structure

### Core Documentation

#### [System Overview](system_overview.md)
**Primary Entry Point** - Comprehensive high-level guide covering:
- Complete system architecture with diagrams
- Streamlined API usage and examples
- Configuration management and environment variables
- Performance characteristics and benchmarks
- Use case recommendations and best practices
- CLI tools and management interfaces
- Security considerations and migration paths

#### [Cache Handlers](cache_handlers.md) 
**Backend Reference** - Detailed cache backend documentation:
- All supported cache backends (PostgreSQL, Redis, RocksDB)
- Multi-partition support and datatype management
- Performance characteristics and memory efficiency
- Backend-specific features and API reference
- Migration and troubleshooting guides

#### [Queue System](queue_system.md)
**Queue Architecture** - Two-queue system documentation:
- Queue providers (PostgreSQL and Redis)
- Priority processing and concurrency control
- LISTEN/NOTIFY support and real-time processing
- CLI tools and management operations
- Performance tuning and monitoring

#### [PostgreSQL Queue Processor](postgresql_queue_processor.md)
**Modern Processing** - Database-native processing documentation:
- pg_cron integration and automated scheduling
- Comprehensive monitoring and logging
- Concurrency control and error recovery
- Installation, configuration, and troubleshooting
- Performance optimization and best practices

#### [Datatype Support](datatype_support.md)
**Reference Guide** - Compatibility matrix and datatype information:
- Backend datatype support matrix
- Conversion guidelines and recommendations
- Performance implications of different datatypes
- Usage examples and best practices

#### [Integration Test Guide](integration_test_guide.md)
**Testing Documentation** - Comprehensive testing guide:
- Test structure and coverage
- Local and CI/CD execution differences
- Environment setup and dependencies
- Running specific test suites
- Troubleshooting and debugging

## Quick Start Guide

### 1. System Overview
Start with [System Overview](system_overview.md) for architecture understanding and API introduction.

### 2. Choose Your Backend
Consult [Cache Handlers](cache_handlers.md) to select the optimal cache backend for your use case:
- **PostgreSQL Array**: Mixed datatypes, full SQL features
- **PostgreSQL Bit**: Integer-only, memory efficient
- **Redis**: Distributed, high throughput
- **RocksDB**: File-based, development/embedded

### 3. Configuration
Follow the configuration examples in [System Overview](system_overview.md) for environment setup.

### 4. Processing Model
Choose between:
- **PostgreSQL Queue Processor**: Modern, database-native ([PostgreSQL Queue Processor](postgresql_queue_processor.md))
- **Monitor Processing**: Traditional, external scripts ([Queue System](queue_system.md))

### 5. Implementation
Use the streamlined API examples throughout the documentation for implementation.

## Documentation Philosophy

This documentation has been restructured to:
- **Eliminate Duplication**: Each topic is covered comprehensively in one place
- **Focus on Current State**: No references to old versions or historical changes
- **Provide Practical Guidance**: Emphasis on implementation and best practices
- **Include Comprehensive Diagrams**: Visual representations of system architecture
- **Maintain Accuracy**: All information reflects the current codebase state

## Getting Help

- **System Architecture**: See [System Overview](system_overview.md)
- **Backend Selection**: See [Cache Handlers](cache_handlers.md) 
- **Queue Setup**: See [Queue System](queue_system.md)
- **PostgreSQL Queue Processor**: See [PostgreSQL Queue Processor](postgresql_queue_processor.md)
- **Datatype Issues**: See [Datatype Support](datatype_support.md)

For examples and practical workflows, refer to the `examples/` directory in the project root.

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