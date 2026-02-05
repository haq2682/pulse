# Apache NiFi Configuration for Pulse

This directory contains NiFi flow templates and configurations for the Pulse data ingestion system.

## Directory Structure

```
nifi/
├── README.md                           # This file
├── templates/                          # NiFi flow templates
│   ├── batch_ingestion_flow.json      # Batch file upload flow
│   ├── db_streaming_flow.json         # Database streaming flow
│   └── api_polling_flow.json          # API polling flow
├── custom_processors/                  # Custom NiFi processors (optional)
└── flows/                              # Exported NiFi flows (XML)
```

## Quick Start

### 1. Access NiFi UI

```bash
# Ensure services are running
docker-compose up -d

# Access NiFi at:
# URL: http://localhost:8081/nifi
# Username: admin
# Password: adminadminadmin
```

### 2. Install Prerequisites

```bash
# Install PostgreSQL JDBC driver
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
exit

# Restart NiFi
docker restart nifi
```

### 3. Import Flow Templates

The templates in this directory are **reference configurations** in JSON format. To build the flows in NiFi:

1. Open the NiFi UI
2. Read the template JSON file for your desired mode
3. Create processors manually according to the template specifications
4. Configure processor properties as documented
5. Connect processors according to the relationships

**Note**: NiFi template import/export uses XML format. These JSON files are documentation that describes how to build the flows.

## Available Templates

### 1. Batch Ingestion Flow

**File**: `templates/batch_ingestion_flow.json`

**Purpose**: Receives file uploads from frontend, validates, stores in MinIO, and tracks in PostgreSQL.

**Key Features**:
- HTTP listener on port 8082
- File format validation (CSV, Excel, JSON, Parquet)
- Automatic retry on failure
- PostgreSQL metadata tracking

**Test**:
```bash
curl -X POST http://localhost:8082/upload \
  -F "file=@test.csv" \
  -F "fileId=$(uuidgen)" \
  -F "fileName=test.csv" \
  -F "userId=user-1" \
  -F "businessId=business-1"
```

### 2. Database Streaming Flow

**File**: `templates/db_streaming_flow.json`

**Purpose**: Queries external databases incrementally and publishes to Kafka in canonical format.

**Key Features**:
- Incremental queries with max-value tracking
- Multiple table support (15 canonical tables)
- Canonical Kafka message format
- Configurable polling interval

**Setup**:
1. Configure external database connection pool
2. Duplicate processor branch for each table
3. Update table names and Kafka topics

**Test**:
```bash
# Verify Kafka messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecom.customers \
  --from-beginning
```

### 3. API Polling Flow

**File**: `templates/api_polling_flow.json`

**Purpose**: Polls external API endpoints and publishes to Kafka in canonical format.

**Key Features**:
- Configurable polling interval
- API format validation
- Automatic retry on failure
- Support for 15 canonical tables

**Setup**:
1. Update `api.endpoint.url` variable
2. Configure authentication if required
3. Adjust polling interval

**Test**:
```bash
# Create test API (see NIFI_SETUP_GUIDE.md for example)
# Verify Kafka messages as above
```

## Configuration Variables

Set these in NiFi UI → Process Group → Variables:

### Batch Mode
```
(No variables needed - uses environment variables)
```

### DB Mode
```
db.connection.url = jdbc:postgresql://external-host:5432/external_db
db.driver.class = org.postgresql.Driver
db.driver.location = /opt/nifi/nifi-current/lib/postgresql-42.6.0.jar
db.username = debezium_user
db.password = password
```

### API Mode
```
api.endpoint.url = http://external-api.com/api/data
api.username = (if required)
api.password = (if required)
api.poll.interval = 10 sec
```

## Controller Services

### PostgreSQL Connection Pool (Batch Mode)

```
Service: DBCPConnectionPool
Name: PostgreSQLConnectionPool
Properties:
  - Database Connection URL: jdbc:postgresql://10.5.0.5:5432/${env:POSTGRES_DB}
  - Database Driver Class Name: org.postgresql.Driver
  - Database User: ${env:POSTGRES_USER}
  - Database Password: ${env:POSTGRES_PASSWORD}
  - Max Total Connections: 10
```

### External DB Connection Pool (DB Mode)

```
Service: DBCPConnectionPool
Name: ExternalDBConnectionPool
Properties:
  - Database Connection URL: ${db.connection.url}
  - Database Driver Class Name: ${db.driver.class}
  - Database User: ${db.username}
  - Database Password: ${db.password}
  - Max Total Connections: 5
```

### JSON Reader/Writer

```
Service: JsonTreeReader
Name: JsonTreeReader
Properties:
  - Schema Access Strategy: Infer Schema

Service: JsonRecordSetWriter
Name: JsonRecordSetWriter
Properties:
  - Schema Write Strategy: full-schema-attribute
```

## Environment Variables

Available from `docker-compose.yml`:

```bash
MINIO_ENDPOINT=${env:MINIO_ENDPOINT}
MINIO_ACCESS_KEY=${env:MINIO_ACCESS_KEY}
MINIO_SECRET_KEY=${env:MINIO_SECRET_KEY}
POSTGRES_USER=${env:POSTGRES_USER}
POSTGRES_PASSWORD=${env:POSTGRES_PASSWORD}
POSTGRES_DB=${env:POSTGRES_DB}
POSTGRES_SERVER=${env:POSTGRES_SERVER}
KAFKA_BOOTSTRAP=${env:KAFKA_BOOTSTRAP}
```

Use in NiFi: `${env:VARIABLE_NAME}`

## Canonical Schema

### 15 Tables

1. addresses
2. cart_items
3. categories
4. customer_sessions
5. customers
6. inventory
7. marketing_campaigns
8. order_items
9. orders
10. payments
11. products
12. reviews
13. shopping_cart
14. suppliers
15. wishlist

### Kafka Topics

| Table | Topic |
|-------|-------|
| addresses | ecom.addresses |
| cart_items | ecom.cart_items |
| categories | ecom.categories |
| customer_sessions | ecom.customer_sessions |
| customers | ecom.customers |
| inventory | ecom.inventory |
| marketing_campaigns | ecom.marketing_campaigns |
| order_items | ecom.order_items |
| orders | ecom.orders |
| payments | ecom.payments |
| products | ecom.products |
| reviews | ecom.reviews |
| shopping_cart | ecom.shopping_cart |
| suppliers | ecom.suppliers |
| wishlist | ecom.wishlist |

## Canonical Message Format

From `mapping/streaming/canonical_message.py`:

```json
{
  "source_type": "db" | "api",
  "vendor": "custom",
  "table": "customers",
  "schema_version": "v1",
  "timestamp": "2026-02-05T10:30:45Z",
  "operation": "c|u|d|r",
  "payload": {
    "customer_id": "123",
    "name": "Alice",
    "email": "alice@example.com"
  }
}
```

**CDC Operations**:
- `c` = create (new record)
- `r` = read (snapshot/initial load)
- `u` = update (record updated)
- `d` = delete (record deleted)

## Monitoring

### Key Metrics

**Batch Mode**:
- HandleHttpRequest: Number of uploads
- PutS3Object: Success rate
- ExecuteSQL: Insert latency

**DB Mode**:
- QueryDatabaseTableRecord: Records fetched
- PublishKafka: Messages published
- Database connection pool: Active connections

**API Mode**:
- InvokeHTTP: API call success rate
- ValidateJSON: Validation success rate
- PublishKafka: Messages published

### Recommended Alerts

1. Alert if file validation failure rate > 10%
2. Alert if S3 upload failure rate > 5%
3. Alert if Kafka publish failure rate > 5%
4. Alert if retry count > threshold
5. Alert if database connection pool exhausted

## Troubleshooting

### NiFi Not Starting

```bash
# Check logs
docker logs -f nifi

# Common issues:
# - Insufficient memory (increase Docker memory to 4GB+)
# - Port conflict (check if 8081 is in use)
# - Permission issues (check volume permissions)
```

### Cannot Upload Files

```bash
# Check HandleHttpRequest is running
# Check port 8082 is accessible
curl -v http://localhost:8082/upload

# Check NiFi logs for errors
docker logs nifi | grep ERROR
```

### Database Connection Fails

```bash
# Test connection from NiFi container
docker exec -it nifi bash
psql -h 10.5.0.5 -U ${POSTGRES_USER} -d ${POSTGRES_DB}

# Verify JDBC driver is installed
ls -la /opt/nifi/nifi-current/lib/ | grep postgres
```

### Kafka Messages Not Publishing

```bash
# Check Kafka is running
docker ps | grep kafka

# Verify broker is reachable from NiFi
docker exec -it nifi bash
nc -zv 10.5.0.7 9092

# Check Kafka topics exist
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Performance Tuning

### JVM Settings

Add to `docker-compose.yml`:
```yaml
environment:
  NIFI_JVM_HEAP_INIT: 4g
  NIFI_JVM_HEAP_MAX: 8g
```

### Backpressure Thresholds

Configure on connections:
```
Object Threshold: 10000
Size Threshold: 1 GB
```

### Concurrent Tasks

Increase for high-throughput processors:
```
Concurrent Tasks: 4-8
```

### Rate Limiting

Add `ControlRate` processor:
```
Rate Control Criteria: flowfile count
Maximum Rate: 1000
Time Duration: 1 min
```

## Security

### Enable HTTPS

```yaml
# docker-compose.yml
environment:
  NIFI_WEB_HTTPS_PORT: 8443
  NIFI_WEB_HTTP_PORT: ""  # Disable HTTP
```

### Authentication

For production, configure LDAP or OAuth:
```yaml
environment:
  NIFI_SECURITY_USER_LDAP_URL: ldap://ldap-server:389
  NIFI_SECURITY_USER_LDAP_MANAGER_DN: cn=admin,dc=example,dc=com
```

### MinIO Access

Use IAM roles instead of access keys in production.

### Kafka Security

Enable SASL/SSL:
```
security.protocol: SASL_SSL
sasl.mechanism: PLAIN
```

## Next Steps

1. ✅ Review NIFI_SETUP_GUIDE.md for detailed setup instructions
2. ✅ Build batch ingestion flow first
3. ✅ Test with sample file upload
4. ✅ Update frontend to use NiFi endpoint
5. ⏳ Build DB streaming flow
6. ⏳ Build API polling flow
7. ⏳ Set up monitoring and alerts
8. 🔮 Production hardening (HTTPS, clustering, etc.)

## Related Documentation

- **NIFI_SETUP_GUIDE.md**: Comprehensive setup guide (root directory)
- **mapping/README.md**: Mapping pipeline documentation
- **mapping/API_AND_FILE_INGESTION_GUIDE.md**: API format specifications
- **docker-compose.yml**: Infrastructure configuration

## Support

For issues or questions:
1. Check NIFI_SETUP_GUIDE.md troubleshooting section
2. Review NiFi logs: `docker logs nifi`
3. Check NiFi UI bulletins (top-right corner)
4. Review processor logs and attributes

---

**Version**: 1.0.0
**Last Updated**: 2026-02-05
**NiFi Version**: 2.7.2 (Java 21.0.9+11-LTS)
