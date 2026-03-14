# Architecture & Business Process Flow

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Application Layer                                    │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Client  │  Pipeline  │  Multi (Transaction)  │  PubSub  │  Script  │ FailoverClient │
└────┬────────────┬────────────┬────────────────────┬────────────┬────────────┬────┘
     │            │            │                    │            │            │
     └────────────┴────────────┴────────────────────┴────────────┴────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Connection Pool (ConnPool)                              │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐  Semaphore (pool_size)     │
│  │ Idle    │ │ Idle    │ │ Idle    │ ... │ Idle    │  Idle timeout eviction     │
│  │ Conn 1  │ │ Conn 2  │ │ Conn 3  │     │ Conn N  │                             │
│  └────┬────┘ └────┬────┘ └────┬────┘     └────┬────┘                             │
└───────┼──────────┼──────────┼────────────────┼──────────────────────────────────┘
        │          │          │                │
        └──────────┴──────────┴────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Connection (TcpStream + BufReader)                        │
│  • AUTH / SELECT on init  • Read/Write timeouts  • RESP serialization             │
└─────────────────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Parser (RESP Protocol)                               │
│  Value types: Status, Error, Int, BulkString, Nil, Array                         │
└─────────────────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Redis Server (TCP :6379)                             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Business Process Flow

### Single Command Execution Flow

```mermaid
flowchart TD
    A[Client.process_cmd] --> B[Acquire connection from pool]
    B --> C{Pool has idle conn?}
    C -->|Yes| D[Reuse idle connection]
    C -->|No| E[Acquire semaphore permit]
    E --> F[Dial new TCP connection]
    F --> G{AUTH/SELECT needed?}
    G -->|Yes| H[Run init: AUTH, SELECT]
    G -->|No| I[Connection ready]
    H --> I
    D --> I
    I --> J[Serialize args to RESP]
    J --> K[Write to connection]
    K --> L[Parse reply]
    L --> M[Return connection to pool]
    M --> N[Return Value to caller]
```

### Pipeline Execution Flow

```mermaid
flowchart TD
    A[Pipeline.execute] --> B[Acquire connection from pool]
    B --> C[Serialize all commands to single buffer]
    C --> D[Write buffer in one shot]
    D --> E[Read reply 1]
    E --> F[Read reply 2]
    F --> G[...]
    G --> H[Read reply N]
    H --> I[Return Vec of Values]
```

### Transaction (MULTI/EXEC) Flow

```mermaid
flowchart TD
    A[Multi.exec] --> B[Acquire connection from pool]
    B --> C[Write MULTI]
    C --> D[Write queued commands]
    D --> E[Write EXEC]
    E --> F[Read MULTI OK]
    F --> G[Read QUEUED × N]
    G --> H[Read EXEC result]
    H --> I{Result empty?}
    I -->|Yes| J[Return TxFailed - WATCH triggered]
    I -->|No| K[Return array of results]
```

### Pub/Sub Flow

```mermaid
flowchart TD
    A[PubSub.subscribe] --> B[Acquire connection]
    B --> C[Send SUBSCRIBE/PSUBSCRIBE]
    C --> D[Connection held - not returned to pool]
    D --> E[PubSub.receive]
    E --> F[Parse reply - subscription or message]
    F --> G{Message type?}
    G -->|Subscription| H[Return Subscription confirmation]
    G -->|message| I[Return Message]
    G -->|pmessage| J[Return PMessage]
    H --> E
    I --> E
    J --> E
```

### Lua Script Execution Flow

```mermaid
flowchart TD
    A[Script.run] --> B[Try EVALSHA with hash]
    B --> C{Success?}
    C -->|Yes| D[Return result]
    C -->|NOSCRIPT| E[Fallback: EVAL with full source]
    E --> F[Return result]
```

### Sentinel Failover Flow

```mermaid
flowchart TD
    A[FailoverClient.get_client] --> B[get_master_addr]
    B --> C[Try each Sentinel address]
    C --> D[SENTINEL get-master-addr-by-name]
    D --> E{Response valid?}
    E -->|No| C
    E -->|Yes| F[Cache master addr]
    F --> G[Create Client with master addr]
    G --> H[Return Client]
```
