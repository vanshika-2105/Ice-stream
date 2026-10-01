# Security Audit

| Service | Configuration | Potential issue | Action taken |
|---|---|---|---|
| Kafka | `apache/kafka:4.0.1`; INTERNAL, EXTERNAL and CONTROLLER listeners; host port `9092`; replication factor `1` | PLAINTEXT listeners and host exposure are suitable for local development but should be reviewed before production use | Reviewed; retain current configuration for local development |
| MinIO | `minio/minio:latest`; ports `9000` and `9001`; root credentials supplied through `.env` | `latest` image tag is not pinned; API and console are exposed to the host; local development credentials must not be reused for production | Credentials moved to `.env`; `.env` added to `.gitignore`; review image tag and exposed ports |
| Iceberg REST | `tabulario/iceberg-rest:latest`; port `8181`; MinIO and JDBC credentials supplied through `.env` | `latest` image tag is not pinned; REST port is exposed to the host | Credentials moved to `.env`; review image tag and port |
| Flink JobManager | `ice-stream-flink:1.20.1-py`; port `8081`; job and dependency volumes mounted | Flink UI/API is exposed to the host | Reviewed; retain host exposure for local development and monitoring |
| Flink TaskManager | `ice-stream-flink:1.20.1-py`; internal communication with JobManager; 2 task slots | No host port exposed | Retain internal-only configuration |
| Backend | Not defined in `infra/docker-compose.yml`; runs separately | Requires separate configuration review | Reviewed separately from Docker Compose |
| Dashboard | Not defined in `infra/docker-compose.yml`; runs separately | Requires separate configuration review | Reviewed separately from Docker Compose |

## Secret Configuration

Configurable credentials are supplied through the local `.env` file rather than being hardcoded directly in Docker Compose.

Current environment variables include:

- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `CATALOG_JDBC_USER`
- `CATALOG_JDBC_PASSWORD`

The `.env` file is excluded from Git through `.gitignore`.

The `.env.example` file contains variable names only and must not contain real credentials.

Local development credentials should not be reused for production deployments.

## Image Version Review

The Compose configuration uses the following image versions:

- Kafka: `apache/kafka:4.0.1`
- Flink: `ice-stream-flink:1.20.1-py`
- MinIO: `minio/minio:latest`
- Iceberg REST: `tabulario/iceberg-rest:latest`

Kafka and Flink use pinned versions.

MinIO and Iceberg REST currently use `latest`. These image tags should be reviewed and pinned to known working versions before production deployment.

No unnecessary infrastructure image changes should be made while the current development environment is being validated.

## Kafka Configuration Review

Kafka uses:

- INTERNAL listener: `kafka:29092`
- EXTERNAL listener: `localhost:9092`
- CONTROLLER listener: `kafka:9093`
- Host port: `9092`
- Single-node controller/broker configuration
- Replication factor: `1`
- Minimum ISR: `1`
- Three Kafka partitions

The current PLAINTEXT configuration is appropriate for the local development environment. Production deployment would require a separate security review covering authentication, encryption, listener configuration, and replication.

## Exposed Port Review

The following services expose host ports:

| Service | Host port | Purpose |
|---|---:|---|
| Kafka | `9092` | Local producer/client access |
| MinIO | `9000` | S3-compatible API |
| MinIO | `9001` | MinIO console |
| Iceberg REST | `8181` | REST catalog access |
| Flink JobManager | `8081` | Flink Web UI/API |

The Flink TaskManager does not expose a host port and communicates internally through the Docker network.

The currently exposed ports are retained because they are used for local development, testing, monitoring, or application connectivity. Unnecessary host exposure should be removed for production deployments.

## MinIO Review

MinIO credentials are supplied through environment variables:

```yaml
MINIO_ROOT_USER: ${MINIO_ROOT_USER}
MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}