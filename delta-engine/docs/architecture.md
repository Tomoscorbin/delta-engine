# Architecture Overview

This page shows the high-level design. It focuses on the main components, their roles, and the sync flow.

## Component/Class Diagram

```mermaid
classDiagram
    class Engine {
      +sync(registry)
    }
    class Registry {
      +register(*tables)
      +__iter__()
    }
    class CatalogStateReader {
      <<protocol>>
      +fetch_state(qualified_name)
    }
    class PlanExecutor {
      <<protocol>>
      +execute(plan)
    }
    class PlanValidator {
      +validate(context)
    }
    class DatabricksReader {
      +fetch_state(qualified_name)
    }
    class DatabricksExecutor {
      +execute(plan)
    }
    class Column
    class DesiredTable
    class ObservedTable
    class Action
    class CreateTable
    class AddColumn
    class DropColumn
    class ActionPlan {
      target
      actions
    }

    Engine --> Registry : iterate desired tables
    Engine --> CatalogStateReader : read state
    Engine --> PlanValidator : validate plan
    Engine --> PlanExecutor : execute actions

    CatalogStateReader <|.. DatabricksReader : implements
    PlanExecutor <|.. DatabricksExecutor : implements

    Registry o--> DesiredTable : contains
    DesiredTable o--> Column : columns
    ObservedTable o--> Column : columns

    Action <|-- CreateTable
    Action <|-- AddColumn
    Action <|-- DropColumn
    ActionPlan o--> Action : 0..*
```

```mermaid
sequenceDiagram
    autonumber
    participant U as User code
    participant R as Registry
    participant E as Engine
    participant CR as CatalogStateReader
    participant V as PlanValidator
    participant X as PlanExecutor

    U->>R: register(DeltaTable)
    E->>R: iterate desired tables
    loop per table
        E->>CR: fetch_state(qualified_name)
        CR-->>E: ReadResult (present/absent/failure)
        E->>V: diff + order -> ActionPlan  ->validate(plan)
        V-->>E: failures?
        alt plan valid
            E->>X: execute(plan)
            X-->>E: ExecutionResults
        else validation failed
            E-->>E: skip execution
        end
    end
    E-->>U: report or SyncFailedError
```


Notes:
- The engine is backend-agnostic via small ports (`CatalogStateReader`, `PlanExecutor`).
- Plans are deterministic (create → adds → drops; subjects alphabetical).
- Validation runs before execution to catch obvious mistakes early.
