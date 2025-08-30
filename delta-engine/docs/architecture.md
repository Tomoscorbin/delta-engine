# Architecture Overview

This page shows the high-level design. It focuses on the main components, their roles, and the sync flow.

## Component/Class Diagram

```mermaid
classDiagram
    %% === Ports (interfaces the Engine talks to) ===
    class Registry {
      +register(*tables)
      +__iter__()
    }
    class CatalogStateReader {
      +fetch_state(qualified_name)
    }
    class Differ {
      +diff(desired: DesiredTable, observed: ObservedTable) ActionPlan
    }
    class PlanValidator {
      +validate(plan)
    }
    class PlanExecutor {
      +execute(plan)
    }

    %% === Adapters (implementations) ===
    class DatabricksStateReader
    class DatabricksExecutor

    %% === Domain types ===
    class DesiredTable
    class ObservedTable
    class ActionPlan {
      target
      actions
    }
    class Action
    class CreateTable
    class AddColumn
    class DropColumn
    class Engine {
      +sync(registry)
    }

    %% Engine depends on ports (dotted = lightweight dependency)
    Engine ..> Registry : iterates
    Engine ..> CatalogStateReader : reads state
    Engine ..> Differ : builds plan
    Engine ..> PlanValidator : validates
    Engine ..> PlanExecutor : executes

    %% Port -> Adapter realizations
    CatalogStateReader <|.. DatabricksStateReader
    PlanExecutor <|.. DatabricksExecutor

    %% Data/model relations
    Registry o-- DesiredTable : contains

    %% Differ inputs/outputs (as deps to avoid heavy graph)
    Differ ..> DesiredTable : input
    Differ ..> ObservedTable : input
    Differ --> ActionPlan : output

    %% Plan structure
    ActionPlan o-- Action : 0..*
    Action <|-- CreateTable
    Action <|-- AddColumn
    Action <|-- DropColumn
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
        CR-->>E: ReadResult
        E->>V: ActionPlan  -> validate plan
        E->>X: execute plan
        X-->>U: ExecutionResults
    end
```


Notes:
- The engine is backend-agnostic via small ports (`CatalogStateReader`, `PlanExecutor`).
- Plans are deterministic (create → adds → drops; subjects alphabetical).
- Validation runs before execution to catch obvious mistakes early.
