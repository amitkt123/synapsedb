# SynapseDB Architecture

## Overview

SynapseDB follows a layered architecture with clear separation of concerns:
```
┌─────────────────────────────────────┐
│   Public API Layer                  │  ← IndexService, SearchService
├─────────────────────────────────────┤
│   Domain Logic Layer                │  ← Index, Document, Query
├─────────────────────────────────────┤
│   Lucene Abstraction Layer          │  ← IndexWriter, IndexReader wrappers
├─────────────────────────────────────┤
│   Apache Lucene                     │  ← Core search library
└─────────────────────────────────────┘
```

## Core Principles

1. **Independence**: Core module has no external dependencies except Lucene
2. **Simplicity**: Start simple, add complexity progressively
3. **Testability**: Every component is unit testable
4. **Extensibility**: Plugin architecture for future enhancements

## Module Structure

### synapsedb-core (Phase 1-3)
Pure Java + Lucene implementation of search engine

### synapsedb-server (Phase 4)
Spring Boot REST API wrapper around core

### synapsedb-client (Phase 4)
Java client library for easy integration

## Design Patterns Used

- **Facade Pattern**: SynapseDB class as main entry point
- **Builder Pattern**: Request/Response builders
- **Strategy Pattern**: Query types
- **Factory Pattern**: Analyzer and tokenizer creation

(More details to be added as implementation progresses)
