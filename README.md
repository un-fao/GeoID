# Agro-Informatics Platform - Catalog Services

Agro-Informatics Platform (AIP) - Catalog Services is an enterprise-grade, cloud-native platform for the management, processing, and dissemination of geospatial data. 

The system (powered by the internal dynastore engine) is designed as a modular, microservices-based framework engineered for extreme scalability, maintainability, and interoperability, strictly adhering to modern software engineering principles and **Open Geospatial Consortium (OGC)** standards.

## Architecture

Our core philosophy rests on a strict separation of concerns, a principle that permeates every layer of the system. We utilize "Three Pillars" (Modules, Extensions, Tasks) mitigating technical debt and ensuring system resilience. 

A key innovation is the database architecture, which leverages advanced features of **PostgreSQL** and **PostGIS**, including a "lazy" on-demand partitioning strategy orchestrated by database triggers. This provides true multi-tenancy and performance isolation scaling to trillions of features without degradation.

### Table of Contents

**Part 1: Foundational Concepts & Architecture**
- [Architecture Overview](docs/architecture/overview.md)
- [The Database Layer](docs/architecture/database.md)
- [The Query Executor Pattern](docs/architecture/query_executor.md)
- [Distributed Tasks](docs/architecture/distributed-tasks.md)

**Part 2: Deep Dive into Modules and Extensions**
- [The Catalog Module](docs/components/catalog.md)
- [The Asynchronous Task Ecosystem](docs/components/tasks.md)
- [The GCP Extension](docs/components/gcp.md)
- [The OGC Features Extension](docs/components/features.md)
- [The STAC Extension](docs/components/stac.md)
- [The Legacy WFS Extension](docs/components/wfs.md)
- [Elasticsearch Integration](docs/components/elasticsearch.md)

**Part 3: Extending & Contributing**
- [Contributing & Plugin Naming Convention](docs/contributing.md)
- [Example Project Template](examples/my-project/) — starter kit for downstream projects
- [Roadmap](docs/roadmap.md)

**Testing**
- [Coverage Report](docs/testing/coverage-report.md) — per-module coverage, priorities, duplication analysis

---
*For AI guidelines and constraints, see `.ai_context.md` files localized within specific source directories.*
