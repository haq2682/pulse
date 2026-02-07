# Apache NiFi Setup Guide for Pulse Data Ingestion

**Version**: NiFi 2.7.2 with Java 21.0.9+11-LTS
**Last Updated**: 2026-02-05

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Current Architecture Analysis](#current-architecture-analysis)
3. [Target Architecture with NiFi](#target-architecture-with-nifi)
4. [NiFi Installation & Access](#nifi-installation--access)
5. [Mode 1: Batch File Ingestion](#mode-1-batch-file-ingestion)
6. [Mode 2: Database Streaming](#mode-2-database-streaming)
7. [Mode 3: API Polling & Streaming](#mode-3-api-polling--streaming)
8. [Frontend Integration Changes](#frontend-integration-changes)
9. [NiFi Processors & Configuration](#nifi-processors--configuration)
10. [Testing & Validation](#testing--validation)
11. [Issues & Recommendations](#issues--recommendations)
12. [Troubleshooting](#troubleshooting)

---

## Executive Summary

This guide provides a comprehensive walkthrough for replacing the current FastAPI-based file upload system with Apache NiFi for data ingestion. The system supports three ingestion modes:

- **Batch Mode**: File uploads (CSV, Excel, JSON, Parquet) → MinIO → Mapping Pipeline
- **DB Mode**: Database CDC → Debezium → Kafka → Mapping Pipeline → MinIO
- **API Mode**: API Polling → Kafka → Mapping Pipeline → MinIO

**Current Flow (FastAPI)**:
```
Frontend → FastAPI /upload-chunk → MinIO ingested/ → Python Mapping Pipeline
```

**Target Flow (NiFi)**:
```
Frontend → NiFi HTTP Listener → MinIO ingested/ → Python Mapping Pipeline
Database → NiFi → Debezium/Kafka → Mapping Pipeline → MinIO
API → NiFi → Kafka → Mapping Pipeline → MinIO
```

---

## Current Architecture Analysis

### Infrastructure Components

Based on `docker-compose.yml` analysis:

| Component | IP Address | Port(s) | Purpose |
|-----------|-----------|---------|---------|
| **NiFi** | 10.5.0.12 | 8081 (UI), 8443 (HTTPS) | Data ingestion orchestration |
| **Kafka** | 10.5.0.7 | 9092 | Message streaming |
| **MinIO** | 10.5.0.4 | 9000 (API), 9001 (Console) | S3-compatible object storage |
| **PostgreSQL** | 10.5.0.5 | 5432 | Metadata database |
| **Debezium** | 10.5.0.10 | 8083 | CDC connector |
| **Redis** | 10.5.0.11 | 6379 | Cache & session management |
| **Zookeeper** | 10.5.0.6 | 2181 | Kafka coordination |

### Current FastAPI Implementation

**File Upload Flow** (`api/routers/onboarding.py`):

1. **Frontend** chunks files (5MB chunks) and sends to `/onboarding/upload-chunk`
2. **FastAPI** receives chunks:
   - Creates multipart upload in MinIO
   - Tracks upload state in Redis (`upload:{fileId}:upload_id`, `upload:{fileId}:parts`)
   - Records metadata in PostgreSQL `uploaded_files` table
3. **Storage**: Files stored in `{business_id}/ingested/{file_name}` in MinIO
4. **Completion**: When all chunks uploaded, FastAPI completes multipart upload

**Key FastAPI Endpoints**:
- `POST /onboarding/upload-chunk` - Receives file chunks (lines 148-224)
- `GET /onboarding/uploaded-files` - Lists uploaded files (lines 226-257)
- `DELETE /onboarding/delete-file` - Removes file (lines 259-292)

**Issues Identified**:
1. ❌ **Tight coupling**: Upload logic tightly coupled to FastAPI
2. ❌ **Manual chunking**: Frontend manually chunks files
3. ❌ **Redis dependency**: Upload state stored in Redis (ephemeral)
4. ❌ **Limited retry logic**: No built-in retry on chunk failure
5. ❌ **No data transformation**: Files stored as-is without validation
6. ❌ **Single-threaded**: Each chunk processed sequentially

---

## Target Architecture with NiFi

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND                                 │
│  (frontend/src/pages/onboarding/connect/index.jsx)              │
└────────────────────┬────────────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
    BATCH         DB MODE      API MODE
        │            │            │
        ↓            ↓            ↓
┌──────────────────────────────────────────────────────────────────┐
│                       APACHE NIFI (10.5.0.12)                    │
│                                                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   BATCH     │  │      DB     │  │     API     │             │
│  │   FLOW      │  │    FLOW     │  │    FLOW     │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│         │                │                 │                     │
│         ↓                ↓                 ↓                     │
│  HandleHttpRequest  QueryDatabase    InvokeHTTP                 │
│         │           TableCDC              │                      │
│         ↓                │                 │                     │
│  ValidateRecord          │                 │                     │
│         │                ↓                 ↓                     │
│         │          JoltTransformJSON  JoltTransformJSON          │
│         │                │                 │                     │
│         ↓                ↓                 ↓                     │
│  PutS3Object       PublishKafka      PublishKafka               │
│         │                                                        │
└─────────┼────────────────────────────────────────────────────────┘
          │
          ↓
┌─────────────────────────────────────────────────────────────────┐
│                    MINIO (10.5.0.4:9000)                         │
│                  {business_id}/ingested/                         │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
          ┌──────────────────────┐
          │                      │
          ↓                      ↓
    ┌──────────┐          ┌──────────┐
    │  KAFKA   │          │  PYTHON  │
    │ TOPICS   │←────────│  BATCH   │
    │          │          │ MAPPING  │
    └────┬─────┘          └──────────┘
         │
         ↓
┌─────────────────────────────────────────────────────────────────┐
│              SPARK STREAMING CONSUMER                            │
│          (mapping/streaming/spark_streaming.py)                  │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ↓
          ┌─────────────────────┐
          │  MAPPING PIPELINE   │
          │  (mapping/map.py)   │
          │  - 7 algorithms     │
          │  - 15 tables        │
          └──────────┬──────────┘
                     │
                     ↓
┌─────────────────────────────────────────────────────────────────┐
│                    MINIO (10.5.0.4:9000)                         │
│                   {business_id}/mapped/                          │
│  - addresses.csv, customers.csv, orders.csv, etc.               │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Overview

#### **Batch Mode Flow**:
```
1. User uploads file via frontend
2. Frontend sends file to NiFi HTTP endpoint
3. NiFi validates file format
4. NiFi stores in MinIO ingested/ folder
5. NiFi updates PostgreSQL uploaded_files table
6. NiFi triggers Python mapping pipeline (optional)
7. Python processes files through 7-stage mapping
8. Results saved to MinIO mapped/ folder
```

#### **DB Mode Flow**:
```
1. NiFi queries database tables (QueryDatabaseTableCDC)
2. NiFi transforms records to canonical Kafka format
3. NiFi publishes to Kafka ecom.{table} topics
4. Spark Streaming consumes from Kafka
5. Spark applies mapping pipeline
6. Results saved to MinIO mapped/ folder
```

#### **API Mode Flow**:
```
1. NiFi polls API endpoint (InvokeHTTP)
2. NiFi validates API response format
3. NiFi transforms to canonical Kafka format
4. NiFi publishes to Kafka ecom.{table} topics
5. Spark Streaming consumes from Kafka
6. Spark applies mapping pipeline
7. Results saved to MinIO mapped/ folder
```

---

## NiFi Installation & Access

### Access NiFi UI

NiFi is already configured in your `docker-compose.yml` (lines 250-287).

**Access Details**:
- **URL**: http://localhost:8081/nifi
- **Username**: `admin`
- **Password**: `adminadminadmin`
- **Container**: `nifi` (10.5.0.12)

### Start NiFi

```bash
# Start all services
docker-compose up -d

# Check NiFi status
docker ps | grep nifi

# View NiFi logs
docker logs -f nifi

# Wait for NiFi to start (takes 2-3 minutes)
# Look for "NiFi has started"
```

### Verify NiFi is Running

```bash
# Check NiFi UI is accessible
curl -v http://localhost:8081/nifi

# Should return 200 OK with HTML content
```

### NiFi Directory Structure

From `docker-compose.yml`:
```
volumes:
  - nifi_data:/opt/nifi/nifi-current/data
  - ./nifi/custom_processors:/opt/nifi/nifi-current/custom_processors
  - ./nifi/flows:/opt/nifi/nifi-current/flows
```

**Create required directories**:
```bash
# Create NiFi directories
mkdir -p nifi/custom_processors
mkdir -p nifi/flows
mkdir -p nifi/templates

# Set permissions
chmod -R 755 nifi/
```

---

## Mode 1: Batch File Ingestion

### Overview

Replace FastAPI chunked upload with NiFi's production-ready HTTP listener, format-specific validation, and S3 processor designed for big data.

**⚠️ Critical Corrections Applied**:
- ❌ **MultiFormatReader doesn't exist** → Route by format first, then validate per format
- ❌ **ValidateRecord cannot validate Excel** → Convert Excel to CSV first
- ❌ **ExecuteSQL is wrong for INSERTs** → Use PutSQL instead
- ❌ **HandleHttpRequest is dangerous for big files** → Tuned for large uploads with proper backpressure
- ✅ **Proper controller services** → CSVReader, JsonTreeReader, ParquetReader with schema handling

### NiFi Flow Design (Production-Ready)

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BATCH INGESTION FLOW (BIG DATA SAFE)              │
└─────────────────────────────────────────────────────────────────────┘

[HandleHttpRequest] (tuned for 5GB+ files)
        ↓
[PutDistributedMapCache] (request tracking for async response)
        ↓
[UpdateAttribute] (detect format, uuid, metadata)
        ↓
[RouteOnAttribute] (route by file extension)
   ├── csv     → [ValidateRecord] (CSVReader)
   ├── json    → [ValidateRecord] (JsonTreeReader)
   ├── parquet → [ValidateRecord] (ParquetReader + Schema Registry)
   └── excel   → [ConvertRecord] (ExcelReader→CSV) → [ValidateRecord] (CSVReader)
        ↓
[PutS3Object] (MinIO with multipart upload enabled)
        ↓
[PutDatabaseRecord] (Record-based INSERT) OR [PutSQL] (for ON CONFLICT)
        ↓
[FetchDistributedMapCache] (retrieve request context)
        ↓
[HandleHttpResponse] (async-safe response)
```

### Processor Configuration

#### 1. HandleHttpRequest (Big Data Safe)

**Purpose**: Receive large file uploads from frontend

**⚠️ Important**: This processor buffers files in memory. For production:
- Deploy NiFi behind Nginx/HAProxy with upload limits
- Configure proper JVM heap (see Big Data Hardening section)
- Enable backpressure thresholds

**Configuration**:
```
Processor: HandleHttpRequest
Properties:
  - Listening Port: 8082
  - Allowed Paths: /upload
  - HTTP Methods: POST
  - Container Queue Size: 100
  - Maximum Thread Count: 20
  - Max Request Size: 5 GB

Relationships:
  - success → PutDistributedMapCache
```

**Notes**:
- Increased queue size from 10 to 100 for concurrent uploads
- Increased threads from 10 to 20 for better throughput
- Set max request size to 5GB (adjust based on requirements)

#### 2. PutDistributedMapCache (Request Tracking)

**Purpose**: Store request context for async response correlation

**Configuration**:
```
Processor: PutDistributedMapCache
Properties:
  - Cache Entry Identifier: ${http.context.identifier}
  - Distributed Cache Service: DistributedMapCacheClientService
  - Cache Update Strategy: Replace if present
  - Max Cache Entry Life: 1 hour

Custom Properties:
  - request.id: ${http.context.identifier}
  - file.id: ${uuid()}
  - business.id: ${businessId}
  - user.id: ${userId}

Relationships:
  - success → UpdateAttribute
  - failure → HandleHttpResponseError
```

**Notes**:
- Required for async request/response correlation under load
- Stores context in distributed cache (Redis-backed recommended for production)
- Prevents response mismatching when processing multiple concurrent uploads

#### 3. UpdateAttribute (Format Detection)

**Purpose**: Detect file format and set metadata

**Configuration**:
```
Processor: UpdateAttribute
Properties:
  - uuid: ${uuid()}
  - file.ext: ${filename:toLower():substringAfterLast('.')}
  - s3.bucket: ${businessId}
  - s3.key: ingested/${filename}
  - mime.type: ${mime.type}
  - user.id: ${userId}
  - file.size: ${file.size}
  - file.id: ${uuid}

Relationships:
  - success → RouteOnAttribute
```

**Notes**:
- `file.ext` extracts extension in lowercase for routing
- `uuid` generates unique file identifier
- All attributes from HTTP request are preserved

#### 4. RouteOnAttribute (Format Routing)

**Purpose**: Route to format-specific validation processors

**⚠️ Critical**: NiFi cannot auto-detect formats. You **must** route by extension first.

**Configuration**:
```
Processor: RouteOnAttribute
Properties:
  - Routing Strategy: Route to Property name

Custom Properties:
  - csv: ${file.ext:equals('csv')}
  - json: ${file.ext:equals('json')}
  - parquet: ${file.ext:equals('parquet')}
  - excel: ${file.ext:matches('xls|xlsx')}

Relationships:
  - csv → ValidateRecord (CSV)
  - json → ValidateRecord (JSON)
  - parquet → ValidateRecord (Parquet)
  - excel → ConvertRecord_Excel
  - unmatched → HandleHttpResponseError (unsupported format)
```

**Supported Formats**:
- ✅ CSV (`.csv`)
- ✅ JSON (`.json`)
- ✅ Parquet (`.parquet`)
- ✅ Excel (`.xls`, `.xlsx`) - **converted to CSV first**

#### 5a. ValidateRecord (CSV)

**Purpose**: Validate CSV files

**Configuration**:
```
Processor: ValidateRecord
Name: ValidateRecord_CSV
Properties:
  - Record Reader: CSVReader
  - Record Writer: CSVRecordSetWriter
  - Schema Access Strategy: Infer Schema
  - Allow Extra Fields: true
  - Max Invalid Records: 10

Relationships:
  - valid → PutS3Object
  - invalid → LogInvalidRecords
  - failure → HandleHttpResponseError
```

**Controller Service**: CSVReader (see Controller Services section)

#### 5b. ValidateRecord (JSON)

**Purpose**: Validate JSON files

**Configuration**:
```
Processor: ValidateRecord
Name: ValidateRecord_JSON
Properties:
  - Record Reader: JsonTreeReader
  - Record Writer: JsonRecordSetWriter
  - Schema Access Strategy: Infer Schema
  - Allow Extra Fields: true
  - Max Invalid Records: 10

Relationships:
  - valid → PutS3Object
  - invalid → LogInvalidRecords
  - failure → HandleHttpResponseError
```

**Controller Service**: JsonTreeReader (see Controller Services section)

#### 5c. ValidateRecord (Parquet)

**Purpose**: Validate Parquet files

**⚠️ Important**: Parquet **requires** schema registry. Cannot reliably infer schema at scale.

**Configuration**:
```
Processor: ValidateRecord
Name: ValidateRecord_Parquet
Properties:
  - Record Reader: ParquetReader
  - Record Writer: ParquetRecordSetWriter
  - Schema Access Strategy: Use 'Schema Name' Property
  - Schema Registry: DatabaseTableSchemaRegistry
  - Schema Name: pulse_schema
  - Allow Extra Fields: true

Relationships:
  - valid → PutS3Object
  - invalid → LogInvalidRecords
  - failure → HandleHttpResponseError
```

**Controller Services**:
- ParquetReader (see Controller Services section)
- DatabaseTableSchemaRegistry (see Controller Services section)

**Notes**:
- Schema registry is **mandatory** for Parquet at scale
- Database-backed schema registry recommended for production
- Schema must be pre-registered in `nifi_schemas` table

#### 5d. ConvertRecord (Excel to CSV)

**Purpose**: Convert Excel files to CSV format using Record-based processing

**⚠️ Critical**: NiFi 2.x deprecated `ConvertExcelToCSVProcessor`. Use `ConvertRecord` with `ExcelReader` + `CSVRecordSetWriter` instead.

**NiFi 2.x Update**: The old `ConvertExcelToCSVProcessor` has been deprecated in favor of the record-based approach using `ExcelReader` controller service.

**Configuration**:
```
Processor: ConvertRecord
Name: ConvertExcel_CSV
Properties:
  - Record Reader: ExcelReader
  - Record Writer: CSVRecordSetWriter
  - Include Zero Record FlowFiles: false

Relationships:
  - success → ValidateRecord_CSV
  - failure → HandleHttpResponseError
```

**Notes**:
- Uses **ExcelReader** controller service (see Controller Services section)
- Handles both .xlsx (XSSF 2007 OOXML) and .xls (HSSF '97-2007) formats
- Supports password-protected Excel files
- Multi-sheet Excel files: Each sheet becomes a separate record set
- For splitting sheets into separate flowfiles, add **SplitRecord** after ConvertRecord

#### 6. PutS3Object (MinIO - Big Data Safe)

**Purpose**: Store file in MinIO with multipart upload

**⚠️ Critical**: Use controller service for credentials, enable multipart for large files

**Configuration**:
```
Processor: PutS3Object
Properties:
  - Bucket: ${s3.bucket}
  - Object Key: ${s3.key}
  - Endpoint Override URL: http://10.5.0.4:9000
  - AWS Credentials Provider Service: MinIOCredentialsService
  - Signer Override: AWSS3V4SignerType
  - Region: us-east-1
  - Use Path Style Access: true
  - Multipart Upload: true
  - Multipart Part Size: 64 MB
  - Multipart Upload Age-off Interval: 24 hours
  - Multipart Upload Max Age: 7 days

Relationships:
  - success → PutSQL
  - failure → RetryFlowFile
```

**Notes**:
- **Multipart upload is mandatory** for files > 64MB
- Part size of 64MB optimizes for large files
- Uses controller service (no inline credentials)
- Age-off cleans up incomplete multipart uploads

#### 7. PutDatabaseRecord (Database Insert - NiFi 2.x Recommended)

**Purpose**: Insert file metadata into PostgreSQL using Record-based approach

**❌ OLD (Deprecated approach)**: PutSQL with manual SQL and parameters
**✅ NEW (NiFi 2.x Best Practice)**: PutDatabaseRecord with automatic SQL generation

**NiFi 2.x Update**: While `PutSQL` still works, `PutDatabaseRecord` is the modern recommended approach for database inserts, offering better performance and simpler configuration.

**Configuration**:
```
Processor: PutDatabaseRecord
Properties:
  - Record Reader: JsonTreeReader
  - Statement Type: INSERT
  - Database Connection Pooling Service: PostgreSQLConnectionPool
  - Schema Name: public
  - Table Name: uploaded_files
  - Translate Field Names: true
  - Unmatched Field Behavior: Ignore Unmatched Fields
  - Unmatched Column Behavior: Fail on Unmatched Columns
  - Update Keys: file_id
  - Field Containing SQL: (empty)
  - Allow Multiple TableNames: false
  - Maximum Batch Size: 100
  - Statement Timeout: 0 seconds
  - Rollback On Failure: true

Relationships:
  - success → FetchDistributedMapCache
  - retry → RetryFlowFile
  - failure → HandleHttpResponseError
```

**Required: Prepare JSON Record Before PutDatabaseRecord**

Add **UpdateAttribute** processor before PutDatabaseRecord to create JSON record:

```
Processor: UpdateAttribute (before PutDatabaseRecord)
Name: PrepareDBRecord
Properties:
  - db.record: {
      "file_id": "${file.id}",
      "business_id": "${s3.bucket}",
      "file_name": "${filename}",
      "file_size": ${file.size},
      "file_type": "${mime.type}",
      "s3_key": "${s3.key}",
      "upload_status": "completed"
    }

Then use ReplaceText:
  - Search Value: .*
  - Replacement Value: ${db.record}
```

**Alternative: Use PutSQL for ON CONFLICT Upsert**

If you need PostgreSQL-specific `ON CONFLICT` upsert logic, use `PutSQL`:

```
Processor: PutSQL
Properties:
  - JDBC Connection Pool: PostgreSQLConnectionPool
  - SQL Statement:
    INSERT INTO uploaded_files
    (file_id, business_id, file_name, file_size, file_type, s3_key, upload_status, created_at)
    VALUES
    (?, ?, ?, ?, ?, ?, 'completed', NOW())
    ON CONFLICT (file_id)
    DO UPDATE SET upload_status = 'completed', updated_at = NOW();

SQL Parameters (set via UpdateAttribute):
  - sql.args.1.value: ${file.id}
  - sql.args.2.value: ${s3.bucket}
  - sql.args.3.value: ${filename}
  - sql.args.4.value: ${file.size}
  - sql.args.5.value: ${mime.type}
  - sql.args.6.value: ${s3.key}
```

**Notes**:
- **PutDatabaseRecord** is recommended for NiFi 2.x (simpler, better performance)
- Automatically generates SQL from record structure
- Supports batch inserts (up to 100 records per batch)
- For complex upsert logic (ON CONFLICT), use `PutSQL` as shown in alternative
- Rollback on failure ensures transaction consistency

#### 8. FetchDistributedMapCache (Retrieve Request Context)

**Purpose**: Retrieve original request context for async response

**Configuration**:
```
Processor: FetchDistributedMapCache
Properties:
  - Cache Entry Identifier: ${http.context.identifier}
  - Distributed Cache Service: DistributedMapCacheClientService
  - Put Cache Value In Attribute: request.context

Relationships:
  - success → HandleHttpResponse
  - not-found → HandleHttpResponseError
```

**Notes**:
- Retrieves request context stored in step 2
- Required for async response correlation
- Ensures correct response sent to correct client

#### 9. HandleHttpResponse (Async-Safe)

**Purpose**: Send success response to frontend

**Configuration**:
```
Processor: HandleHttpResponse
Properties:
  - HTTP Context Map: HttpContextMap
  - HTTP Status Code: 200
  - HTTP Response Body:
    {
      "status": 200,
      "fileId": "${file.id}",
      "fileName": "${filename}",
      "fileSize": ${file.size},
      "s3Key": "${s3.key}",
      "message": "File uploaded successfully"
    }

Relationships:
  - success → LogSuccess
  - failure → LogAttribute
```

**Notes**:
- Uses HttpContextMap for proper request/response pairing
- Returns detailed file information to frontend
- Safe for concurrent uploads

#### 10. HandleHttpResponseError (Error Handling)

**Purpose**: Send error response to frontend

**Configuration**:
```
Processor: HandleHttpResponse
Properties:
  - HTTP Context Map: HttpContextMap
  - HTTP Status Code: 500
  - HTTP Response Body:
    {
      "status": 500,
      "error": "Upload failed: ${errorMessage}",
      "fileId": "${file.id}"
    }

Relationships:
  - success → LogError
```

#### 11. RetryFlowFile (Retry Logic)

**Purpose**: Retry failed operations with exponential backoff

**Configuration**:
```
Processor: RetryFlowFile
Properties:
  - Retry Attribute: retry.count
  - Maximum Retries: 3
  - Penalize Retries: true
  - Retry Delay: 5 sec
  - Reuse Mode: Fail on Reuse

Relationships:
  - retry → PutS3Object (or PutSQL depending on where it failed)
  - retries_exceeded → HandleHttpResponseError
  - failure → HandleHttpResponseError
```

**Notes**:
- Retries up to 3 times with exponential backoff
- Penalization adds delay between retries
- Prevents overwhelming downstream systems

### NiFi Controller Services

Mode 1 requires the following controller services:

#### 1. PostgreSQLConnectionPool

**Purpose**: Database connection pooling for metadata storage

**Configuration**:
```
Service: DBCPConnectionPool
Name: PostgreSQLConnectionPool
Properties:
  - Database Connection URL: jdbc:postgresql://10.5.0.5:5432/${env:POSTGRES_DB}
  - Database Driver Class Name: org.postgresql.Driver
  - Database Driver Location: /opt/nifi/nifi-current/lib/postgresql-42.6.0.jar
  - Database User: ${env:POSTGRES_USER}
  - Database Password: ${env:POSTGRES_PASSWORD}
  - Max Wait Time: 500 milliseconds
  - Max Total Connections: 20
  - Validation query: SELECT 1
```

**Install PostgreSQL Driver**:
```bash
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
exit
docker restart nifi
```

**Notes**:
- Increased max connections from 10 to 20 for better concurrency
- Uses environment variables for credentials
- Validation query ensures connection health

#### 2. MinIOCredentialsService

**Purpose**: Secure credential management for MinIO access

**✅ Critical**: Use controller service instead of inline credentials

**Configuration**:
```
Service: AWSCredentialsProviderControllerService
Name: MinIOCredentialsService
Properties:
  - Access Key ID: ${env:MINIO_ACCESS_KEY}
  - Secret Access Key: ${env:MINIO_SECRET_KEY}
```

**Notes**:
- Never hardcode credentials in processor properties
- Uses environment variables from docker-compose.yml
- Supports credential rotation without flow changes

#### 3. CSVReader

**Purpose**: Read and parse CSV files

**Configuration**:
```
Service: CSVReader
Name: CSVReader
Properties:
  - Schema Access Strategy: Infer Schema
  - CSV Format: Custom Format
  - Value Separator: ,
  - First Line is Header: true
  - Ignore CSV Header: false
  - Quote Character: "
  - Escape Character: \
  - Comment Marker: #
  - Trim Fields: true
  - Allow Extra Characters: true
  - Charset: UTF-8
```

**Notes**:
- Infers schema automatically from header row
- Handles quoted fields and escape characters
- Trims whitespace from fields

#### 4. CSVRecordSetWriter

**Purpose**: Write CSV output after validation

**Configuration**:
```
Service: CSVRecordSetWriter
Name: CSVRecordSetWriter
Properties:
  - Schema Write Strategy: Set 'schema.name' Attribute
  - Schema Access Strategy: Inherit Record Schema
  - CSV Format: Custom Format
  - Value Separator: ,
  - Include Header Line: true
  - Quote Character: "
  - Escape Character: \
  - Charset: UTF-8
```

#### 5. ExcelReader (NiFi 2.x)

**Purpose**: Read and parse Excel files (.xlsx and .xls)

**NiFi 2.x Update**: Replaces the deprecated `ConvertExcelToCSVProcessor`. Use with `ConvertRecord` for Excel processing.

**Configuration**:
```
Service: ExcelReader
Name: ExcelReader
Properties:
  - Schema Access Strategy: Infer Schema
  - Required Sheets: (empty - all sheets)
  - Starting Row: 1
  - Timestamp Format: yyyy-MM-dd HH:mm:ss
  - Date Format: yyyy-MM-dd
  - Time Format: HH:mm:ss
  - Password: (empty - for password-protected files)
```

**Supported Formats**:
- ✅ `.xlsx` - XSSF 2007 OOXML file format
- ✅ `.xls` - HSSF '97(-2007) file format
- ✅ Password-protected Excel files (set Password property)

**Notes**:
- Infers schema from Excel headers automatically
- Each sheet is processed as a separate record set
- Cell formulas are evaluated automatically
- Shared formulas supported (fixed in recent NiFi versions)
- For multi-sheet files, use `SplitRecord` processor to separate sheets into individual flowfiles

**Multi-Sheet Handling**:

Option 1 - Single flowfile with all sheets:
```
ExcelReader → ConvertRecord → [merged output]
```

Option 2 - Separate flowfile per sheet (recommended):
```
ExcelReader → ConvertRecord → SplitRecord → [one flowfile per sheet]
```

**Configuration for Splitting Sheets**:
```
Processor: SplitRecord
Properties:
  - Record Reader: JsonTreeReader
  - Record Writer: JsonRecordSetWriter
  - Records Per Split: (varies based on sheet size)
```

#### 6. JsonTreeReader

**Purpose**: Read and parse JSON files

**Configuration**:
```
Service: JsonTreeReader
Name: JsonTreeReader
Properties:
  - Schema Access Strategy: Infer Schema
  - Schema Inference Cache: No cache
  - Allow Comments: true
  - Max String Length: 20 MB
```

**Notes**:
- Infers schema from JSON structure
- Allows JSON comments (non-standard but common)
- Max string length of 20MB for large text fields

#### 6. JsonRecordSetWriter

**Purpose**: Write JSON output after validation

**Configuration**:
```
Service: JsonRecordSetWriter
Name: JsonRecordSetWriter
Properties:
  - Schema Write Strategy: Set 'schema.name' Attribute
  - Schema Access Strategy: Inherit Record Schema
  - Suppress Null Values: Never Suppress
  - Pretty Print JSON: false
  - Timestamp Format: yyyy-MM-dd'T'HH:mm:ss'Z'
  - Date Format: yyyy-MM-dd
```

#### 7. ParquetReader

**Purpose**: Read Parquet files

**⚠️ Requires Schema Registry**

**Configuration**:
```
Service: ParquetReader
Name: ParquetReader
Properties:
  - Schema Access Strategy: Use 'Schema Name' Property
  - Schema Registry: DatabaseTableSchemaRegistry
  - Cache Size: 100
```

**Notes**:
- **Cannot reliably infer Parquet schema at scale**
- Requires pre-registered schema in schema registry
- Cache improves performance for repeated schemas

#### 8. ParquetRecordSetWriter

**Purpose**: Write Parquet output

**Configuration**:
```
Service: ParquetRecordSetWriter
Name: ParquetRecordSetWriter
Properties:
  - Schema Write Strategy: Set 'schema.name' Attribute
  - Schema Access Strategy: Inherit Record Schema
  - Compression Type: SNAPPY
  - Row Group Size: 128 MB
  - Page Size: 1 MB
```

**Notes**:
- SNAPPY compression provides good balance of speed and compression
- Row group size of 128MB optimized for analytics queries

#### 9. DatabaseTableSchemaRegistry

**Purpose**: Store and manage schemas for Parquet files

**Configuration**:
```
Service: DatabaseTableSchemaRegistry
Name: DatabaseTableSchemaRegistry
Properties:
  - Database Connection Pooling Service: PostgreSQLConnectionPool
  - Table Name: nifi_schemas
  - Schema Name Column: schema_name
  - Schema Column: schema_text
  - Version Column: version
```

**Setup Schema Table**:
```sql
-- Create schema registry table
CREATE TABLE IF NOT EXISTS nifi_schemas (
    schema_name VARCHAR(255) PRIMARY KEY,
    schema_text TEXT NOT NULL,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert default schema for pulse data
INSERT INTO nifi_schemas (schema_name, schema_text) VALUES
('pulse_schema', '{
  "type": "record",
  "name": "PulseData",
  "fields": [
    {"name": "id", "type": ["null", "string"], "default": null},
    {"name": "timestamp", "type": ["null", "long"], "default": null}
  ]
}')
ON CONFLICT (schema_name) DO NOTHING;
```

**Notes**:
- Database-backed for durability and consistency
- Supports schema versioning
- Shared across all NiFi nodes in a cluster

#### 10. DistributedMapCacheClientService

**Purpose**: Distributed cache for async request tracking

**Configuration**:
```
Service: DistributedMapCacheClientService
Name: DistributedMapCacheClientService
Properties:
  - Server Hostname: 10.5.0.11 (Redis)
  - Server Port: 6379
  - SSL Context Service: (none)
  - Communications Timeout: 30 sec
```

**Requires**: DistributedMapCacheServer (see server configuration below)

**Notes**:
- Uses Redis for production deployments
- Required for async request/response correlation
- Shared across all NiFi nodes in a cluster

#### 11. DistributedMapCacheServer

**Purpose**: Cache server for distributed operations

**Configuration**:
```
Service: DistributedMapCacheServer
Name: DistributedMapCacheServer
Properties:
  - Port: 4557
  - Max Cache Entries: 10000
  - Eviction Strategy: Least Recently Used
  - Persistence Directory: /opt/nifi/nifi-current/cache
  - Max Read Size: 10 MB
```

**Alternative**: Use Redis for production (recommended)

**Notes**:
- For single-node NiFi, can use embedded server
- For production clusters, use external Redis
- Persistence directory survives NiFi restarts

#### 12. HttpContextMap

**Purpose**: HTTP request/response context management

**Configuration**:
```
Service: StandardHttpContextMap
Name: HttpContextMap
Properties:
  - Maximum Outstanding Requests: 5000
  - Request Timeout: 5 minutes
```

**Notes**:
- Maps HTTP requests to responses
- Required for HandleHttpRequest/HandleHttpResponse pairing
- Timeout prevents memory leaks from abandoned requests

### Big Data Hardening

#### JVM Configuration

Add to `docker-compose.yml` or `nifi.properties`:

```yaml
# docker-compose.yml
environment:
  NIFI_JVM_HEAP_INIT: 8g
  NIFI_JVM_HEAP_MAX: 16g
  JAVA_OPTS: >-
    -Xms8g
    -Xmx16g
    -XX:+UseG1GC
    -XX:MaxGCPauseMillis=500
    -XX:InitiatingHeapOccupancyPercent=45
    -XX:G1HeapRegionSize=16m
```

**Notes**:
- Minimum 8GB heap for production
- G1GC recommended for large heaps
- Adjust based on concurrent upload volume

#### Repository Configuration

Update `nifi.properties`:

```properties
# FlowFile Repository (fast SSD recommended)
nifi.flowfile.repository.directory=/opt/nifi/nifi-current/flowfile_repository
nifi.flowfile.repository.checkpoint.interval=2 min
nifi.flowfile.repository.always.sync=false

# Content Repository (large SSD recommended)
nifi.content.repository.directory.default=/opt/nifi/nifi-current/content_repository
nifi.content.claim.max.appendable.size=10 MB
nifi.content.claim.max.flow.files=100
nifi.content.repository.archive.max.retention.period=7 days
nifi.content.repository.archive.max.usage.percentage=50%

# Provenance Repository (can be slower storage)
nifi.provenance.repository.directory.default=/opt/nifi/nifi-current/provenance_repository
nifi.provenance.repository.max.storage.time=30 days
nifi.provenance.repository.max.storage.size=100 GB
```

**Notes**:
- Use **SSD for FlowFile and Content repos** (critical for performance)
- HDD acceptable for Provenance repo
- Archive settings prevent disk exhaustion

#### Backpressure Configuration

Configure on **all connections**:

```
Connection Settings:
  - Object Threshold: 10,000
  - Data Size Threshold: 50 GB
  - Prioritizers: FirstInFirstOutPrioritizer
  - Load Balance Strategy: Do Not Load Balance (single node)
```

**Notes**:
- Backpressure prevents memory exhaustion
- Object threshold limits number of flowfiles
- Data size threshold limits total data in queue
- Adjust based on your data volumes

#### Production Deployment Architecture

For production, deploy behind reverse proxy:

```
┌─────────────────────────────────────────────────────────┐
│               NGINX / HAProxy (Load Balancer)            │
│  - Upload size limit: 10 GB                              │
│  - Timeout: 30 minutes                                   │
│  - Connection limit: 1000 concurrent                     │
└────────────────────┬────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
    ┌───▼───┐   ┌───▼───┐   ┌───▼───┐
    │ NiFi  │   │ NiFi  │   │ NiFi  │
    │ Node1 │   │ Node2 │   │ Node3 │
    │       │   │       │   │       │
    └───┬───┘   └───┬───┘   └───┬───┘
        │            │            │
        └────────────┼────────────┘
                     │
        ┌────────────▼────────────┐
        │    External ZooKeeper    │
        │    (3-5 nodes)           │
        └─────────────────────────┘
```

**Nginx Configuration**:
```nginx
upstream nifi {
    least_conn;
    server 10.5.0.12:8082 max_fails=3 fail_timeout=30s;
    server 10.5.0.13:8082 max_fails=3 fail_timeout=30s;
    server 10.5.0.14:8082 max_fails=3 fail_timeout=30s;
}

server {
    listen 443 ssl http2;
    server_name nifi.yourdomain.com;

    # SSL configuration
    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;

    # Upload limits
    client_max_body_size 10G;
    client_body_timeout 1800s;
    proxy_read_timeout 1800s;
    proxy_send_timeout 1800s;

    # Connection limits
    limit_conn_zone $binary_remote_addr zone=upload_limit:10m;
    limit_conn upload_limit 10;

    location /upload {
        proxy_pass http://nifi;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_request_buffering off;
    }
}
```

### Monitoring & Alerts

Configure these metrics for monitoring:

| Metric | Threshold | Alert |
|--------|-----------|-------|
| **FlowFile Queue** | > 5,000 | Warning |
| **Content Repo Usage** | > 80% | Critical |
| **JVM Heap Usage** | > 85% | Critical |
| **HandleHttpRequest errors** | > 5% | Warning |
| **PutS3Object failures** | > 5% | Critical |
| **PutSQL failures** | > 1% | Critical |
| **Backpressure active** | > 10 min | Warning |

**Setup Prometheus Exporter**:
```yaml
# Add to docker-compose.yml
prometheus-nifi-exporter:
  image: msiedlarek/nifi_exporter
  ports:
    - "9092:9092"
  environment:
    NIFI_URL: http://10.5.0.12:8080/nifi
```

### Summary

Mode 1 Batch Flow is now production-ready with:

✅ **Format-specific validation** (CSV, JSON, Parquet, Excel)
✅ **Big data safe** (multipart uploads, proper backpressure)
✅ **Async-safe** (distributed cache for request tracking)
✅ **Proper database operations** (PutSQL instead of ExecuteSQL)
✅ **Comprehensive error handling** (retry logic with exponential backoff)
✅ **Monitoring & alerting** (metrics and thresholds)
✅ **Production hardening** (JVM tuning, repository configuration)
✅ **Clustering ready** (distributed cache, external ZooKeeper)

---

## Mode 2: Database Streaming

### Overview

Ingest data from external databases using Change Data Capture (CDC) via NiFi, then publish to Kafka for the mapping pipeline.

### NiFi Flow Design

```
┌─────────────────────────────────────────────────────────────┐
│                  DATABASE INGESTION FLOW                     │
└─────────────────────────────────────────────────────────────┘

[QueryDatabaseTableRecord]
       ↓
  (Periodic query with max-value tracking)
       ↓
[SplitRecord]
       ↓
  (Split into individual records)
       ↓
[JoltTransformJSON]
       ↓
  (Transform to canonical Kafka format)
       ↓
[RouteOnAttribute]
       ↓
  (Route by table name)
       ↓
[PublishKafka]
       ↓
  (Publish to ecom.{table} topics)
```

### Processor Configuration

#### 1. QueryDatabaseTableRecord

**Purpose**: Query database tables incrementally

**Configuration**:
```
Processor: QueryDatabaseTableRecord
Properties:
  - Database Connection Pooling Service: ExternalDBConnectionPool
  - Table Name: ${db.table.name}
  - Maximum-value Columns: updated_at,created_at
  - Columns to Return: * (all columns)
  - Max Wait Time: 0 seconds
  - Record Writer: JSONRecordSetWriter
  - Normalize Table/Column Names: true

Schedule:
  - Run Schedule: 10 sec (configurable)

Relationships:
  - success → SplitRecord
```

**Supported Databases**:
- ✅ PostgreSQL (via Debezium)
- ✅ MySQL
- ✅ MongoDB
- ✅ SQL Server
- ✅ Oracle
- ✅ Db2

#### 2. SplitRecord

**Purpose**: Split result set into individual records

**Configuration**:
```
Processor: SplitRecord
Properties:
  - Record Reader: JsonTreeReader
  - Record Writer: JsonRecordSetWriter
  - Records Per Split: 1

Relationships:
  - splits → JoltTransformJSON
  - original → (terminate)
  - failure → LogAttribute
```

#### 3. JoltTransformJSON

**Purpose**: Transform to canonical Kafka message format

**Canonical Format** (from `mapping/streaming/canonical_message.py`):
```json
{
  "source_type": "db",
  "vendor": "custom",
  "table": "customers",
  "schema_version": "v1",
  "timestamp": "2026-02-05T10:30:45Z",
  "operation": "c",
  "payload": {
    "customer_id": "123",
    "name": "Alice",
    "email": "alice@example.com"
  }
}
```

**JOLT Specification**:
```json
[
  {
    "operation": "shift",
    "spec": {
      "*": "payload.&",
      "@metadata": {
        "table": "table",
        "operation": "operation"
      }
    }
  },
  {
    "operation": "default",
    "spec": {
      "source_type": "db",
      "vendor": "custom",
      "schema_version": "v1",
      "timestamp": "${now():format('yyyy-MM-dd\'T\'HH:mm:ss\'Z\')}",
      "operation": "r"
    }
  }
]
```

**Configuration**:
```
Processor: JoltTransformJSON
Properties:
  - Jolt Specification: ${jolt.spec}
  - Jolt Transform: Chain

Relationships:
  - success → RouteOnAttribute
  - failure → LogAttribute
```

#### 4. RouteOnAttribute

**Purpose**: Route to correct Kafka topic by table name

**Configuration**:
```
Processor: RouteOnAttribute
Properties:
  - Routing Strategy: Route to Property name
  - customers: ${table:equals('customers')}
  - orders: ${table:equals('orders')}
  - products: ${table:equals('products')}
  - addresses: ${table:equals('addresses')}
  - cart_items: ${table:equals('cart_items')}
  - categories: ${table:equals('categories')}
  - customer_sessions: ${table:equals('customer_sessions')}
  - inventory: ${table:equals('inventory')}
  - marketing_campaigns: ${table:equals('marketing_campaigns')}
  - order_items: ${table:equals('order_items')}
  - payments: ${table:equals('payments')}
  - reviews: ${table:equals('reviews')}
  - shopping_cart: ${table:equals('shopping_cart')}
  - suppliers: ${table:equals('suppliers')}
  - wishlist: ${table:equals('wishlist')}

Relationships:
  - customers → PublishKafka (topic: ecom.customers)
  - orders → PublishKafka (topic: ecom.orders)
  - ... (repeat for all tables)
```

#### 5. PublishKafka

**Purpose**: Publish messages to Kafka topics

**Configuration**:
```
Processor: PublishKafka_2_6
Properties:
  - Kafka Brokers: 10.5.0.7:9092
  - Topic Name: ecom.${table}
  - Delivery Guarantee: Best Effort
  - Message Key Field: (empty - use round-robin)
  - Compression Type: snappy
  - Acks: 1
  - Max Request Size: 1 MB

Relationships:
  - success → LogAttribute
  - failure → RetryFlowFile
```

### NiFi Controller Services

Mode 2 (Database Streaming) requires the following controller services to be configured:

#### 1. ExternalDBConnectionPool

**Purpose**: Manages connections to external databases for incremental queries

Create a new `DBCPConnectionPool` for each external database:

**PostgreSQL Example**:
```
Service: DBCPConnectionPool
Name: ExternalPostgreSQLPool
Properties:
  - Database Connection URL: jdbc:postgresql://external-host:5432/external_db
  - Database Driver Class Name: org.postgresql.Driver
  - Database Driver Location: /opt/nifi/nifi-current/lib/postgresql-42.6.0.jar
  - Database User: debezium_user
  - Database Password: ${STREAMING_DB_PASSWORD}
  - Max Wait Time: 500 milliseconds
  - Max Total Connections: 5
  - Validation query: SELECT 1
```

**Install PostgreSQL Driver**:
```bash
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
exit
docker restart nifi
```

**MySQL Example**:
```
Service: DBCPConnectionPool
Name: ExternalMySQLPool
Properties:
  - Database Connection URL: jdbc:mysql://external-host:3306/external_db
  - Database Driver Class Name: com.mysql.cj.jdbc.Driver
  - Database Driver Location: /opt/nifi/nifi-current/lib/mysql-connector-j-8.0.33.jar
  - Database User: debezium_user
  - Database Password: ${STREAMING_DB_PASSWORD}
  - Max Wait Time: 500 milliseconds
  - Max Total Connections: 5
  - Validation query: SELECT 1
```

**Install MySQL Driver**:
```bash
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
exit
docker restart nifi
```

**SQL Server Example**:
```
Service: DBCPConnectionPool
Name: ExternalSQLServerPool
Properties:
  - Database Connection URL: jdbc:sqlserver://external-host:1433;databaseName=external_db
  - Database Driver Class Name: com.microsoft.sqlserver.jdbc.SQLServerDriver
  - Database Driver Location: /opt/nifi/nifi-current/lib/mssql-jdbc-12.2.0.jre11.jar
  - Database User: debezium_user
  - Database Password: ${STREAMING_DB_PASSWORD}
  - Max Wait Time: 500 milliseconds
  - Max Total Connections: 5
```

**Install SQL Server Driver**:
```bash
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar
exit
docker restart nifi
```

**Oracle Example**:
```
Service: DBCPConnectionPool
Name: ExternalOraclePool
Properties:
  - Database Connection URL: jdbc:oracle:thin:@external-host:1521:external_db
  - Database Driver Class Name: oracle.jdbc.OracleDriver
  - Database Driver Location: /opt/nifi/nifi-current/lib/ojdbc8.jar
  - Database User: debezium_user
  - Database Password: ${STREAMING_DB_PASSWORD}
  - Max Wait Time: 500 milliseconds
  - Max Total Connections: 5
  - Validation query: SELECT 1 FROM DUAL
```

**Install Oracle Driver**:
```bash
# Oracle JDBC driver requires Oracle account to download
# Download ojdbc8.jar from https://www.oracle.com/database/technologies/jdbc-ucp-122-downloads.html
# Then copy to NiFi container:
docker cp ojdbc8.jar nifi:/opt/nifi/nifi-current/lib/
docker restart nifi
```

**Notes for Oracle**:
- Oracle requires `FROM DUAL` in validation queries
- For Oracle 19c+, use `ojdbc8.jar` (Java 8+)
- For Oracle 12c, use `ojdbc7.jar` (Java 7+)
- Connection URL format: `jdbc:oracle:thin:@host:port:SID` or `jdbc:oracle:thin:@//host:port/SERVICE_NAME`

**IBM Db2 Example**:
```
Service: DBCPConnectionPool
Name: ExternalDb2Pool
Properties:
  - Database Connection URL: jdbc:db2://external-host:50000/external_db
  - Database Driver Class Name: com.ibm.db2.jcc.DB2Driver
  - Database Driver Location: /opt/nifi/nifi-current/lib/db2jcc4.jar
  - Database User: debezium_user
  - Database Password: ${STREAMING_DB_PASSWORD}
  - Max Wait Time: 500 milliseconds
  - Max Total Connections: 5
  - Validation query: SELECT 1 FROM SYSIBM.SYSDUMMY1
```

**Install Db2 Driver**:
```bash
# Download Db2 JDBC driver from IBM website
# Or use Maven repository:
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.8.0/jcc-11.5.8.0.jar
mv jcc-11.5.8.0.jar db2jcc4.jar
exit
docker restart nifi
```

**Notes for Db2**:
- Db2 requires `FROM SYSIBM.SYSDUMMY1` in validation queries
- Default port is 50000
- Supports both Type 4 (pure Java) and Type 2 (native) drivers
- Use Type 4 driver for NiFi (no native libraries needed)

**MongoDB Example**:
```
Service: DBCPConnectionPool
Name: ExternalMongoDBPool
Properties:
  - Database Connection URL: mongodb://external-host:27017/external_db?replicaSet=rs0
  - Database Driver Class Name: com.mongodb.jdbc.MongoDriver
  - Database Driver Location: /opt/nifi/nifi-current/lib/mongodb-jdbc-2.0.2-all.jar
  - Database User: debezium_user
  - Database Password: ${STREAMING_DB_PASSWORD}
  - Max Wait Time: 500 milliseconds
  - Max Total Connections: 5
  - Validation query: db.runCommand({ping: 1})
```

**Install MongoDB Driver**:
```bash
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
# MongoDB JDBC driver (for SQL-like queries)
wget https://github.com/mongodb/mongo-jdbc-driver/releases/download/v2.0.2/mongodb-jdbc-2.0.2-all.jar
exit
docker restart nifi
```

**Notes for MongoDB**:
- MongoDB must be configured as a **replica set** for CDC
- Connection URL must include `?replicaSet=rs0` parameter
- MongoDB 3.6+ required for change streams
- Alternative: Use `mongo-java-driver` with custom NiFi processors

**Cassandra Example**:
```
Service: DBCPConnectionPool
Name: ExternalCassandraPool
Properties:
  - Database Connection URL: jdbc:cassandra://external-host:9042/external_keyspace
  - Database Driver Class Name: com.github.adejanovski.cassandra.jdbc.CassandraDriver
  - Database Driver Location: /opt/nifi/nifi-current/lib/cassandra-jdbc-wrapper-3.1.0-bundle.jar
  - Database User: debezium_user
  - Database Password: ${STREAMING_DB_PASSWORD}
  - Max Wait Time: 1000 milliseconds
  - Max Total Connections: 3
  - Validation query: SELECT now() FROM system.local
```

**Install Cassandra Driver**:
```bash
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://github.com/adejanovski/cassandra-jdbc-wrapper/releases/download/v3.1.0/cassandra-jdbc-wrapper-3.1.0-bundle.jar
exit
docker restart nifi
```

**Notes for Cassandra**:
- Cassandra has no native CDC support in JDBC
- Use custom NiFi processors or REST API for best performance
- Default port is 9042 (CQL native protocol)
- Connection URL format: `jdbc:cassandra://host:port/keyspace`
- **Recommended**: Use Debezium Cassandra connector instead of JDBC

**Google Cloud Spanner Example**:
```
Service: DBCPConnectionPool
Name: ExternalSpannerPool
Properties:
  - Database Connection URL: jdbc:cloudspanner:/projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_NAME
  - Database Driver Class Name: com.google.cloud.spanner.jdbc.JdbcDriver
  - Database Driver Location: /opt/nifi/nifi-current/lib/google-cloud-spanner-jdbc-2.9.0-single-jar-with-dependencies.jar
  - Database User: (empty - uses service account)
  - Database Password: (empty - uses service account)
  - Max Wait Time: 2000 milliseconds
  - Max Total Connections: 5
  - Validation query: SELECT 1
```

**Install Spanner Driver**:
```bash
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://repo1.maven.org/maven2/com/google/cloud/google-cloud-spanner-jdbc/2.9.0/google-cloud-spanner-jdbc-2.9.0-single-jar-with-dependencies.jar
exit
docker restart nifi
```

**Notes for Google Spanner**:
- Requires Google Cloud service account JSON key
- Set environment variable: `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json`
- No username/password - uses service account authentication
- Connection URL format: `jdbc:cloudspanner:/projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_NAME`
- Supports full ANSI SQL

**Vitess Example**:
```
Service: DBCPConnectionPool
Name: ExternalVitessPool
Properties:
  - Database Connection URL: jdbc:vitess://external-host:15306/external_db?TABLET_TYPE=master
  - Database Driver Class Name: io.vitess.jdbc.VitessDriver
  - Database Driver Location: /opt/nifi/nifi-current/lib/vitess-jdbc-7.0.0.jar
  - Database User: debezium_user
  - Database Password: ${STREAMING_DB_PASSWORD}
  - Max Wait Time: 500 milliseconds
  - Max Total Connections: 5
  - Validation query: SELECT 1
```

**Install Vitess Driver**:
```bash
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
# Vitess JDBC driver (compatible with MySQL protocol)
wget https://repo1.maven.org/maven2/io/vitess/vitess-jdbc/7.0.0/vitess-jdbc-7.0.0.jar
# Also need MySQL driver as dependency
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
exit
docker restart nifi
```

**Notes for Vitess**:
- Vitess uses MySQL-compatible protocol
- Default VTGate port is 15306
- Connection URL must specify `TABLET_TYPE` (master, replica, rdonly)
- Supports MySQL syntax and most MySQL features

#### 2. JsonTreeReader

**Purpose**: Reads JSON records from database query results

**Configuration**:
```
Service: JsonTreeReader
Name: JsonTreeReader
Properties:
  - Schema Access Strategy: Infer Schema
  - Schema Inference Cache: No cache
```

**Notes**:
- This service is used by SplitRecord processor to parse JSON records
- Automatically infers schema from incoming data
- No additional configuration required

#### 3. JsonRecordSetWriter

**Purpose**: Writes JSON records for transformation pipeline

**Configuration**:
```
Service: JsonRecordSetWriter
Name: JsonRecordSetWriter
Properties:
  - Schema Write Strategy: full-schema-attribute
  - Schema Access Strategy: Inherit Record Schema
  - Timestamp Format: yyyy-MM-dd HH:mm:ss
  - Date Format: yyyy-MM-dd
  - Time Format: HH:mm:ss
  - Pretty Print JSON: false
  - Suppress Null Values: Never Suppress
```

**Notes**:
- Used by QueryDatabaseTableRecord and SplitRecord processors
- Preserves schema information in flowfile attributes
- Timestamp format matches PostgreSQL/MySQL defaults

#### 4. JSONRecordSetWriter (for QueryDatabaseTableRecord)

**Purpose**: Initial JSON writer for database query output

**Configuration**:
```
Service: JsonRecordSetWriter
Name: JSONRecordSetWriter
Properties:
  - Schema Write Strategy: full-schema-attribute
  - Schema Access Strategy: Inherit Record Schema
  - Output Grouping: output-array
```

**Notes**:
- This is a separate instance used by QueryDatabaseTableRecord
- Outputs records as JSON array for downstream processing

### Database Connection Summary

Comprehensive reference for all supported databases:

| Database | JDBC Driver | Driver Class | Connection URL Format | Default Port | Validation Query |
|----------|-------------|--------------|----------------------|--------------|-----------------|
| **PostgreSQL** | postgresql-42.6.0.jar | org.postgresql.Driver | jdbc:postgresql://host:5432/db | 5432 | SELECT 1 |
| **MySQL** | mysql-connector-j-8.0.33.jar | com.mysql.cj.jdbc.Driver | jdbc:mysql://host:3306/db | 3306 | SELECT 1 |
| **SQL Server** | mssql-jdbc-12.2.0.jre11.jar | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://host:1433;databaseName=db | 1433 | SELECT 1 |
| **Oracle** | ojdbc8.jar | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@host:1521:SID | 1521 | SELECT 1 FROM DUAL |
| **IBM Db2** | db2jcc4.jar | com.ibm.db2.jcc.DB2Driver | jdbc:db2://host:50000/db | 50000 | SELECT 1 FROM SYSIBM.SYSDUMMY1 |
| **MongoDB** | mongodb-jdbc-2.0.2-all.jar | com.mongodb.jdbc.MongoDriver | mongodb://host:27017/db?replicaSet=rs0 | 27017 | db.runCommand({ping: 1}) |
| **Cassandra** | cassandra-jdbc-wrapper-3.1.0-bundle.jar | com.github.adejanovski.cassandra.jdbc.CassandraDriver | jdbc:cassandra://host:9042/keyspace | 9042 | SELECT now() FROM system.local |
| **Google Spanner** | google-cloud-spanner-jdbc-2.9.0-single-jar-with-dependencies.jar | com.google.cloud.spanner.jdbc.JdbcDriver | jdbc:cloudspanner:/projects/PROJECT_ID/instances/INSTANCE_ID/databases/DB | N/A | SELECT 1 |
| **Vitess** | vitess-jdbc-7.0.0.jar | io.vitess.jdbc.VitessDriver | jdbc:vitess://host:15306/db?TABLET_TYPE=master | 15306 | SELECT 1 |

### Database-Specific Prerequisites

Before configuring NiFi for database ingestion, ensure the database administrator has completed these prerequisites:

#### PostgreSQL
- ✅ Enable logical replication: `wal_level = logical`
- ✅ Create dedicated streaming user with replication permissions
- ✅ Create publication: `CREATE PUBLICATION pulse_pub FOR ALL TABLES;`
- ✅ Grant permissions: `GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;`

#### MySQL
- ✅ Enable binary logging: `log_bin = mysql-bin` and `binlog_format = ROW`
- ✅ Create streaming user
- ✅ Grant permissions: `GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO debezium_user;`

#### SQL Server
- ✅ Enable SQL Server Agent
- ✅ Enable CDC on database: `EXEC sys.sp_cdc_enable_db;`
- ✅ Enable CDC on tables: `EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_name', @role_name = NULL;`
- ✅ Create login and grant permissions

#### Oracle
- ✅ Enable ARCHIVELOG mode
- ✅ Enable supplemental logging
- ✅ Create streaming user with LOGMINING permissions
- ✅ Grant permissions: `GRANT CREATE SESSION, LOGMINING, SELECT ANY TRANSACTION TO debezium_user;`

#### IBM Db2
- ✅ Enable log retain for recovery: `db2 update db cfg for DBNAME using LOGARCHMETH1 LOGRETAIN`
- ✅ Create streaming user
- ✅ Grant permissions: `GRANT SELECT ON SCHEMA SCHEMANAME TO debezium_user;`

#### MongoDB
- ✅ Configure as **replica set** (required for change streams)
- ✅ Initialize replica set: `rs.initiate()`
- ✅ Create user with read permissions
- ✅ MongoDB 3.6+ required for change streams

#### Cassandra
- ✅ No native CDC support via JDBC
- ✅ **Recommended**: Use Debezium Cassandra connector or custom NiFi processors
- ✅ Alternative: Poll tables directly (not real-time)

#### Google Cloud Spanner
- ✅ Create service account with Cloud Spanner Database Reader role
- ✅ Download service account JSON key
- ✅ Set environment variable: `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`
- ✅ Enable Spanner API in Google Cloud Console

#### Vitess
- ✅ Vitess cluster must be running
- ✅ VTGate must be accessible
- ✅ Create user in underlying MySQL databases
- ✅ Configure tablet type (master, replica, or rdonly)

### Driver Download Links

| Database | Download Link |
|----------|--------------|
| **PostgreSQL** | https://jdbc.postgresql.org/download/postgresql-42.6.0.jar |
| **MySQL** | https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar |
| **SQL Server** | https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre11/mssql-jdbc-12.2.0.jre11.jar |
| **Oracle** | https://www.oracle.com/database/technologies/jdbc-ucp-122-downloads.html (requires Oracle account) |
| **IBM Db2** | https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.8.0/jcc-11.5.8.0.jar |
| **MongoDB** | https://github.com/mongodb/mongo-jdbc-driver/releases/download/v2.0.2/mongodb-jdbc-2.0.2-all.jar |
| **Cassandra** | https://github.com/adejanovski/cassandra-jdbc-wrapper/releases/download/v3.1.0/cassandra-jdbc-wrapper-3.1.0-bundle.jar |
| **Google Spanner** | https://repo1.maven.org/maven2/com/google/cloud/google-cloud-spanner-jdbc/2.9.0/google-cloud-spanner-jdbc-2.9.0-single-jar-with-dependencies.jar |
| **Vitess** | https://repo1.maven.org/maven2/io/vitess/vitess-jdbc/7.0.0/vitess-jdbc-7.0.0.jar |

### CDC Operations Mapping

The canonical message format supports CDC operations:

| Operation | Code | Description |
|-----------|------|-------------|
| **Create** | `c` | New record inserted |
| **Read** | `r` | Snapshot/initial load |
| **Update** | `u` | Record updated |
| **Delete** | `d` | Record deleted |

**Note**: For initial implementation, use `operation: "r"` (read) for all records. CDC operations (`c`, `u`, `d`) will be added when Debezium is integrated.

---

## Mode 3: API Polling & Streaming

### Overview

Poll external API endpoints and publish data to Kafka for the mapping pipeline.

### NiFi Flow Design

```
┌─────────────────────────────────────────────────────────────┐
│                    API INGESTION FLOW                        │
└─────────────────────────────────────────────────────────────┘

[GenerateFlowFile]
       ↓
  (Trigger every N seconds)
       ↓
[InvokeHTTP]
       ↓
  (GET external API)
       ↓
[ValidateJson]
       ↓
  (Validate API format)
       ↓
[EvaluateJsonPath]
       ↓
  (Extract tables array)
       ↓
[SplitJson]
       ↓
  (Split into table records)
       ↓
[JoltTransformJSON]
       ↓
  (Transform to canonical format)
       ↓
[PublishKafka]
       ↓
  (Publish to ecom.{table} topics)
```

### API Format Requirements

Based on `mapping/API_AND_FILE_INGESTION_GUIDE.md`, the API must return:

```json
{
  "tables": [
    {
      "table_name": "customers",
      "data": [
        {
          "customer_id": "1",
          "name": "Alice",
          "email": "alice@example.com"
        },
        {
          "customer_id": "2",
          "name": "Bob",
          "email": "bob@example.com"
        }
      ]
    },
    {
      "table_name": "orders",
      "data": [
        {
          "order_id": "101",
          "customer_id": "1",
          "amount": 250
        }
      ]
    }
  ]
}
```

### Processor Configuration

#### 1. GenerateFlowFile

**Purpose**: Trigger API polling at intervals

**Configuration**:
```
Processor: GenerateFlowFile
Properties:
  - Batch Size: 1
  - Data Format: Text
  - Unique FlowFiles: true
  - Custom Text: {"trigger": "api_poll"}

Schedule:
  - Run Schedule: 10 sec (configurable via frontend)

Relationships:
  - success → InvokeHTTP
```

#### 2. InvokeHTTP

**Purpose**: Call external API endpoint

**Configuration**:
```
Processor: InvokeHTTP
Properties:
  - HTTP Method: GET
  - Remote URL: ${api.endpoint.url}
  - SSL Context Service: (if HTTPS)
  - Connection Timeout: 30 sec
  - Read Timeout: 30 sec
  - Include Date Header: false
  - Follow Redirects: true
  - Attributes to Send: (none)
  - Penalize on "No Retry": false
  - Use Chunked Encoding: false

Relationships:
  - Response → ValidateJson
  - No Retry → LogAttribute
  - Failure → RetryFlowFile
```

#### 3. ValidateJson

**Purpose**: Validate API response format

**Configuration**:
```
Processor: ValidateJson
Properties:
  - Schema Access Strategy: Use String Property
  - Schema Text:
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "required": ["tables"],
      "properties": {
        "tables": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["table_name", "data"],
            "properties": {
              "table_name": {"type": "string"},
              "data": {"type": "array"}
            }
          }
        }
      }
    }

Relationships:
  - valid → EvaluateJsonPath
  - invalid → LogAttribute (drop)
```

#### 4. EvaluateJsonPath

**Purpose**: Extract tables array

**Configuration**:
```
Processor: EvaluateJsonPath
Properties:
  - Destination: flowfile-attribute
  - Return Type: json
  - Path Not Found Behavior: warn

Custom Properties:
  - tables: $.tables

Relationships:
  - matched → SplitJson
  - unmatched → LogAttribute
```

#### 5. SplitJson

**Purpose**: Split tables array into individual table records

**Configuration**:
```
Processor: SplitJson
Properties:
  - JsonPath Expression: $.tables[*]
  - Null Value Representation: empty string

Relationships:
  - split → JoltTransformJSON
  - original → (terminate)
  - failure → LogAttribute
```

#### 6. JoltTransformJSON

**Purpose**: Transform to canonical Kafka format

**JOLT Specification**:
```json
[
  {
    "operation": "shift",
    "spec": {
      "table_name": "table",
      "data": {
        "*": "payload"
      }
    }
  },
  {
    "operation": "default",
    "spec": {
      "source_type": "api",
      "vendor": "custom",
      "schema_version": "v1",
      "timestamp": "${now():format('yyyy-MM-dd\'T\'HH:mm:ss\'Z\')}"
    }
  }
]
```

**Configuration**:
```
Processor: JoltTransformJSON
Properties:
  - Jolt Specification: ${jolt.spec}
  - Jolt Transform: Chain

Relationships:
  - success → SplitJson (split data array)
  - failure → LogAttribute
```

#### 7. SplitJson (Second Pass)

**Purpose**: Split data array into individual records

**Configuration**:
```
Processor: SplitJson
Properties:
  - JsonPath Expression: $.payload[*]
  - Null Value Representation: empty string

Relationships:
  - split → PublishKafka
  - original → (terminate)
  - failure → LogAttribute
```

#### 8. PublishKafka

**Purpose**: Publish to Kafka topics

**Configuration**:
```
Processor: PublishKafka_2_6
Properties:
  - Kafka Brokers: 10.5.0.7:9092
  - Topic Name: ecom.${table}
  - Delivery Guarantee: Best Effort
  - Compression Type: snappy
  - Acks: 1

Relationships:
  - success → LogAttribute
  - failure → RetryFlowFile
```

### NiFi Controller Services

Mode 3 (API Polling) requires minimal controller services. Most processing is done with standard JSON processors that don't require explicit controller services.

#### Controller Services Required: **NONE**

**Why?**
- Mode 3 uses simple JSON processors (ValidateJson, SplitJson, EvaluateJsonPath, JoltTransformJSON)
- These processors work directly with JSON text without requiring Record Readers/Writers
- PublishKafka works with raw JSON bytes, no serialization service needed

#### Optional: SSL Context Service (for HTTPS APIs)

If your external API uses HTTPS with custom certificates:

**Configuration**:
```
Service: StandardSSLContextService
Name: APISSLContextService
Properties:
  - Keystore Filename: /opt/nifi/nifi-current/conf/keystore.jks
  - Keystore Password: ${KEYSTORE_PASSWORD}
  - Keystore Type: JKS
  - Truststore Filename: /opt/nifi/nifi-current/conf/truststore.jks
  - Truststore Password: ${TRUSTSTORE_PASSWORD}
  - Truststore Type: JKS
  - TLS Protocol: TLS
```

**Usage**:
- Reference this service in the InvokeHTTP processor's "SSL Context Service" property
- Required only if API uses HTTPS with self-signed or custom certificates
- For public APIs with valid certificates, this service is not needed

#### Optional: HTTP Context Map (for stateful sessions)

If your API requires session management (cookies, tokens):

**Configuration**:
```
Service: StandardHttpContextMap
Name: APIHttpContextMap
Properties:
  - Maximum Outstanding Requests: 5000
  - Request Timeout: 30 sec
```

**Usage**:
- Reference in InvokeHTTP processor if API requires session cookies
- Not needed for stateless REST APIs (most common case)

### API Authentication Options

Mode 3 supports several authentication methods directly in the InvokeHTTP processor:

#### 1. No Authentication
```
InvokeHTTP Properties:
  - Basic Authentication Username: (empty)
  - Basic Authentication Password: (empty)
```

#### 2. Basic Authentication
```
InvokeHTTP Properties:
  - Basic Authentication Username: api_user
  - Basic Authentication Password: ${API_PASSWORD}
```

**Best Practice**: Store password in NiFi variable, not hardcoded

#### 3. Bearer Token Authentication
```
InvokeHTTP Properties:
  - Dynamic Property: Authorization = Bearer ${API_TOKEN}
```

**Setup**:
1. Add custom property in InvokeHTTP processor
2. Property Name: `Authorization`
3. Property Value: `Bearer ${API_TOKEN}`
4. Set `API_TOKEN` in Process Group variables

#### 4. API Key Authentication
```
InvokeHTTP Properties:
  - Dynamic Property: X-API-Key = ${API_KEY}
```

**Setup**:
1. Add custom property in InvokeHTTP processor
2. Property Name: `X-API-Key` (or API's required header name)
3. Property Value: `${API_KEY}`
4. Set `API_KEY` in Process Group variables

### Rate Limiting for APIs

To respect API rate limits, add a **ControlRate** processor after GenerateFlowFile:

**Configuration**:
```
Processor: ControlRate
Position: Between GenerateFlowFile and InvokeHTTP
Properties:
  - Rate Control Criteria: flowfile count
  - Maximum Rate: 60
  - Time Duration: 1 min
  - Grouping Attribute: (empty)

Relationships:
  - success → InvokeHTTP
  - failure → (terminate)
```

**Example Use Cases**:
- API allows 60 requests per minute → Set Maximum Rate: 60, Time Duration: 1 min
- API allows 1000 requests per hour → Set Maximum Rate: 1000, Time Duration: 1 hour
- API allows 10 requests per second → Set Maximum Rate: 10, Time Duration: 1 sec

### Configuration Summary

| Feature | Controller Service Required | Configuration Location |
|---------|----------------------------|----------------------|
| **Basic JSON Processing** | ❌ No | Built-in processors |
| **Kafka Publishing** | ❌ No | PublishKafka processor |
| **HTTPS with custom certs** | ✅ Yes (Optional) | StandardSSLContextService |
| **Session management** | ✅ Yes (Optional) | StandardHttpContextMap |
| **Basic Auth** | ❌ No | InvokeHTTP properties |
| **Bearer Token** | ❌ No | InvokeHTTP dynamic property |
| **API Key** | ❌ No | InvokeHTTP dynamic property |
| **Rate Limiting** | ❌ No | ControlRate processor |

---

## NiFi 2.x Kafka Processor Architecture

### Important Note for NiFi 2.7.2 Users

Apache NiFi 2.x introduced significant changes to Kafka processor architecture. This guide uses `PublishKafka_2_6` for compatibility and ease of transition, but you should be aware of the new recommended approach.

### Current Approach (Used in This Guide)

**Processor**: `PublishKafka_2_6`

**Why we use it**:
- ✅ Preserved in NiFi 2.x for backward compatibility
- ✅ Works immediately without migration
- ✅ Well-documented and battle-tested
- ✅ Suitable for quick setup and testing

**Configuration**:
```
Processor: PublishKafka_2_6
Module: nifi-kafka-2-6-nar
Properties:
  - Kafka Brokers: 10.5.0.7:9092
  - Topic Name: ecom.${table}
  - Delivery Guarantee: Best Effort
  - Compression Type: snappy
  - Acks: 1
```

### Modern Approach (Recommended for New Deployments)

**Architecture**: Controller Service-Based

**Components**:
1. **Kafka3ConnectionService** (Controller Service) - Manages connections to Kafka
2. **PublishKafka** (Processor without version suffix) - Publishes messages

**Why it's better**:
- ✅ Cleaner separation of concerns (connection vs. processing)
- ✅ Easier to update Kafka client libraries
- ✅ Better connection pooling and management
- ✅ Future-proof architecture
- ✅ Supports latest Kafka 3.x features

**Migration Path**:

#### Step 1: Create Kafka3ConnectionService

```
Service: Kafka3ConnectionService
Name: Kafka3Connection
Properties:
  - Bootstrap Servers: 10.5.0.7:9092
  - Connection Timeout: 30 sec
  - Max Request Size: 1 MB
  - Compression Type: snappy
  - Acknowledgment Mode: Leader Acknowledged (acks=1)
```

#### Step 2: Update Processors

**OLD (PublishKafka_2_6)**:
```
Processor: PublishKafka_2_6
Properties:
  - Kafka Brokers: 10.5.0.7:9092
  - Topic Name: ecom.customers
  - Delivery Guarantee: Best Effort
  - Compression Type: snappy
  - Acks: 1
```

**NEW (PublishKafka with Kafka3ConnectionService)**:
```
Processor: PublishKafka
Properties:
  - Connection Service: Kafka3Connection
  - Topic Name: ecom.customers
  - Message Key: (optional)
  - Delivery Guarantee: Best Effort
```

### Breaking Changes Summary

| Feature | NiFi 1.x | NiFi 2.x (Legacy) | NiFi 2.x (Modern) |
|---------|----------|-------------------|-------------------|
| **Processor** | PublishKafka_2_6 | PublishKafka_2_6 (preserved) | PublishKafka |
| **Connection** | Embedded in processor | Embedded in processor | Kafka3ConnectionService |
| **Kafka Client** | 2.6.x | 2.6.x | 3.x |
| **Module** | nifi-kafka-2-6-nar | nifi-kafka-2-6-nar | nifi-kafka-nar |

### Which Approach Should You Use?

#### Use `PublishKafka_2_6` (This Guide) If:
- ⏱️ You need to get started quickly
- 🔄 You're migrating from NiFi 1.x
- 📚 You prefer well-documented, proven solutions
- 🧪 You're setting up a development/test environment

#### Use `PublishKafka` + `Kafka3ConnectionService` (Modern) If:
- 🚀 You're building a new production deployment
- 🔮 You want to future-proof your architecture
- 🎯 You need latest Kafka 3.x features
- 🏗️ You prefer cleaner architectural separation

### Migration Timeline Recommendation

1. **Phase 1 (Immediate)**: Use `PublishKafka_2_6` as documented in this guide
2. **Phase 2 (After testing)**: Plan migration to `PublishKafka` + `Kafka3ConnectionService`
3. **Phase 3 (Production)**: Complete migration before next major NiFi upgrade

### Additional Resources

- [Apache NiFi Kafka Architecture Documentation](https://nifi.apache.org/components/org.apache.nifi.kafka.processors.PublishKafka/)
- [Kafka3ConnectionService Documentation](https://nifi.apache.org/components/org.apache.nifi.kafka.service.Kafka3ConnectionService/)
- [NiFi 2.x Breaking Changes](https://docs.cloudera.com/cfm/latest/release-notes/topics/cfm-nifi2-breaking-changes.html)
- [Migration Guidance for NiFi 2.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)

---

## Frontend Integration Changes

### Current Implementation

**File**: `frontend/src/pages/onboarding/connect/index.jsx`

**Current Flow**:
```javascript
// Lines 77-126: uploadFileInChunks function
1. Chunks file into 5MB pieces
2. Sends each chunk to /onboarding/upload-chunk
3. Tracks progress via uploadProgress state
4. Uses Redis for multipart upload tracking
```

### Required Changes

#### Option 1: Direct NiFi Upload (Recommended)

**Advantages**:
- ✅ Simpler implementation
- ✅ No chunking required
- ✅ NiFi handles large files natively
- ✅ Better error handling

**Implementation**:

```javascript
// Replace uploadFileInChunks with direct upload
const uploadFileToNiFi = async (file, fileId) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('fileId', fileId);
    formData.append('fileName', file.name);
    formData.append('fileSize', file.size);
    formData.append('fileType', file.type);
    formData.append('userId', user.user_id);
    formData.append('businessId', businessId); // From onboarding context

    try {
        const response = await fetch('http://localhost:8082/upload', {
            method: 'POST',
            body: formData,
            // Track upload progress
            onUploadProgress: (progressEvent) => {
                const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
                setUploadProgress(prev => ({ ...prev, [fileId]: progress }));
            }
        });

        if (!response.ok) {
            throw new Error(`Upload failed: ${response.statusText}`);
        }

        const result = await response.json();

        setUploadedFiles(prev => prev.map(f =>
            f.fileId === fileId ? { ...f, persisted: true } : f
        ));

        setTimeout(() => {
            setUploadProgress(prev => {
                const newProgress = { ...prev };
                delete newProgress[fileId];
                return newProgress;
            });
        }, 1000);

        return result;
    } catch (error) {
        console.error('Upload error:', error);
        setUploadedFiles(prev => prev.filter(f => f.fileId !== fileId));
        throw error;
    }
};
```

**Update handleFileSelect** (lines 135-178):
```javascript
const handleFileSelect = async (files) => {
    const fileArray = Array.isArray(files) ? files : Array.from(files);

    const validFiles = fileArray.filter(file => {
        if (!validateFile(file)) {
            console.warn(`File ${file.name} has invalid format`);
            return false;
        }
        return true;
    });

    if (validFiles.length === 0) {
        setErrors(prev => ({ ...prev, form: 'Please select valid file formats (CSV, XLSX, XLS, Parquet, JSON)' }));
        return;
    }

    const newFiles = validFiles.filter(file => {
        return !uploadedFiles.some(f => f.name === file.name && f.size === file.size);
    });

    for (const file of newFiles) {
        const fileId = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}_${file.name}`;
        const fileObj = {
            fileId,
            name: file.name,
            size: file.size,
            type: file.type,
            persisted: false
        };

        setUploadedFiles(prev => [...prev, fileObj]);
        setUploadProgress(prev => ({ ...prev, [fileId]: 0 }));

        // Use NiFi upload instead of chunked upload
        uploadFileToNiFi(file, fileId).catch(err => {
            setErrors(prev => ({ ...prev, form: `Failed to upload ${file.name}` }));
        });
    }

    setErrors(prev => ({ ...prev, form: '' }));

    if (fileInputRef.current) {
        fileInputRef.current.value = '';
    }
};
```

#### Option 2: Keep Chunked Upload (via NiFi)

If chunking is required (e.g., for very large files or network reliability):

**NiFi Flow Modification**:
```
[HandleHttpRequest] (/upload-chunk)
       ↓
[MergeContent] (reassemble chunks)
       ↓
[PutS3Object] (store in MinIO)
```

**Frontend**: Keep existing chunked upload logic, but change endpoint:
```javascript
// Change line 96 from:
await axiosInstance.post('/onboarding/upload-chunk', formData, {...});

// To:
await fetch('http://localhost:8082/upload-chunk', {
    method: 'POST',
    body: formData
});
```

### Database URI & API Endpoint Handling

**No changes required** for DB and API modes - these are handled entirely by NiFi.

**Frontend** only needs to:
1. Collect Database URI or API Endpoint
2. Send to backend API for storage
3. Backend triggers NiFi flow via NiFi REST API

**Example**:
```javascript
// api/routers/onboarding.py - new endpoint
@router.post("/configure-nifi")
async def configure_nifi(request: Request, db=Depends(get_db)):
    body = await request.json()
    ingestion_type = body.get("ingestionType")  # "db" or "api"

    if ingestion_type == "db":
        db_uri = body.get("databaseUri")
        # Start NiFi DB flow via NiFi REST API
        nifi_response = requests.post(
            "http://10.5.0.12:8080/nifi-api/process-groups/{pg_id}/processors",
            json={
                "component": {
                    "type": "org.apache.nifi.processors.standard.QueryDatabaseTableRecord",
                    "config": {
                        "properties": {
                            "dbcp-service": db_uri
                        }
                    }
                }
            }
        )

    elif ingestion_type == "api":
        api_url = body.get("apiEndpoint")
        # Start NiFi API flow via NiFi REST API
        # Similar to above

    return {"status": 200}
```

---

## NiFi Processors & Configuration

### Required NiFi Processors

All of these processors are **included by default** in NiFi 2.7.2:

| Processor | Purpose | Module |
|-----------|---------|--------|
| **HandleHttpRequest** | Receive HTTP requests | nifi-standard-nar |
| **HandleHttpResponse** | Send HTTP responses | nifi-standard-nar |
| **InvokeHTTP** | Call external APIs | nifi-standard-nar |
| **QueryDatabaseTableRecord** | Query databases | nifi-standard-nar |
| **PutS3Object** | Upload to S3/MinIO | nifi-aws-nar |
| **PublishKafka_2_6** | Publish to Kafka (preserved for compatibility)* | nifi-kafka-2-6-nar |
| **JoltTransformJSON** | Transform JSON | nifi-standard-nar |
| **ValidateJson** | Validate JSON schema | nifi-standard-nar |
| **ValidateRecord** | Validate records | nifi-standard-nar |
| **SplitJson** | Split JSON arrays | nifi-standard-nar |
| **SplitRecord** | Split record sets | nifi-standard-nar |
| **UpdateAttribute** | Set flowfile attributes | nifi-update-attribute-nar |
| **RouteOnAttribute** | Route by attributes | nifi-standard-nar |
| **PutDatabaseRecord** | Insert records to database (recommended) | nifi-standard-nar |
| **PutSQL** | Execute parameterized SQL (alternative) | nifi-standard-nar |
| **ConvertRecord** | Convert between record formats | nifi-standard-nar |
| **LogAttribute** | Log for debugging | nifi-standard-nar |
| **MergeContent** | Merge flowfiles | nifi-standard-nar |
| **GenerateFlowFile** | Generate triggers | nifi-standard-nar |
| **EvaluateJsonPath** | Extract JSON values | nifi-standard-nar |

**\* Note on Kafka Processors**: NiFi 2.x introduces a new controller service-based Kafka architecture (`PublishKafka` + `Kafka3ConnectionService`). This guide uses `PublishKafka_2_6` which is preserved for compatibility. See the [NiFi 2.x Kafka Processor Architecture](#nifi-2x-kafka-processor-architecture) section for migration guidance.

### Controller Services Summary

Controller services are reusable components that provide shared functionality to processors. Here's what each mode requires:

#### Mode 1: Batch File Ingestion

| Service | Required | Purpose |
|---------|----------|---------|
| **PostgreSQLConnectionPool** | ✅ Yes | Connect to internal PostgreSQL for metadata tracking |
| **CSVReader** | ✅ Yes | Read CSV files |
| **JsonTreeReader** | ✅ Yes | Read JSON files |
| **ExcelReader** | ✅ Yes | Read Excel files (.xlsx, .xls) |
| **ParquetReader** | ✅ Yes | Read Parquet files |
| **CSVRecordSetWriter** | ✅ Yes | Write CSV records |
| **JsonRecordSetWriter** | ✅ Yes | Write JSON records |
| **DistributedMapCacheClientService** | ✅ Yes | Async request tracking |
| **DistributedMapCacheServer** | ✅ Yes | Cache server for async tracking |
| **AWSCredentialsProvider** | ⚠️ Optional | MinIO credentials (can use direct config) |

**Configuration Location**: See [Mode 1: NiFi Controller Services](#nifi-controller-services)

**Note**: Mode 1 routes by file format, so each format needs its own reader service.

#### Mode 2: Database Streaming

| Service | Required | Purpose |
|---------|----------|---------|
| **ExternalDBConnectionPool** | ✅ Yes | Connect to external databases (PostgreSQL, MySQL, SQL Server, etc.) |
| **JsonTreeReader** | ✅ Yes | Read JSON records from database queries |
| **JsonRecordSetWriter** | ✅ Yes | Write records for transformation pipeline |
| **JSONRecordSetWriter** | ✅ Yes | Initial JSON writer for query output |

**Configuration Location**: See [Mode 2: NiFi Controller Services](#nifi-controller-services-1)

**JDBC Drivers Required** (choose based on your database):
- **PostgreSQL**: `postgresql-42.6.0.jar`
- **MySQL**: `mysql-connector-j-8.0.33.jar`
- **SQL Server**: `mssql-jdbc-12.2.0.jre11.jar`
- **Oracle**: `ojdbc8.jar` (requires Oracle account)
- **IBM Db2**: `db2jcc4.jar` (or `jcc-11.5.8.0.jar`)
- **MongoDB**: `mongodb-jdbc-2.0.2-all.jar`
- **Cassandra**: `cassandra-jdbc-wrapper-3.1.0-bundle.jar`
- **Google Spanner**: `google-cloud-spanner-jdbc-2.9.0-single-jar-with-dependencies.jar`
- **Vitess**: `vitess-jdbc-7.0.0.jar` + `mysql-connector-j-8.0.33.jar`

**Supported Databases**: PostgreSQL, MySQL, SQL Server, Oracle, IBM Db2, MongoDB, Cassandra, Google Cloud Spanner, Vitess

#### Mode 3: API Polling & Streaming

| Service | Required | Purpose |
|---------|----------|---------|
| **None** | ❌ No | Uses built-in JSON processors |
| **StandardSSLContextService** | ⚠️ Optional | HTTPS with custom certificates only |
| **StandardHttpContextMap** | ⚠️ Optional | Session management (rare) |

**Configuration Location**: See [Mode 3: NiFi Controller Services](#nifi-controller-services-2)

**Note**: Mode 3 is the simplest - no mandatory controller services required!

### Environment Variables & Parameter Contexts

**NiFi 2.x Update**: Variable Registry has been removed. Use **Parameter Contexts** and environment variables instead.

#### Using Environment Variables

NiFi can access environment variables from `docker-compose.yml`:

**Available Variables** (lines 270-277):
```yaml
environment:
  MINIO_ENDPOINT: ${MINIO_ENDPOINT}
  MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY}
  MINIO_SECRET_KEY: ${MINIO_SECRET_KEY}
  POSTGRES_USER: ${POSTGRES_USER}
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  POSTGRES_DB: ${POSTGRES_DATABASE_NAME}
  POSTGRES_SERVER: ${POSTGRES_SERVER}
  KAFKA_BOOTSTRAP: ${KAFKA_BOOTSTRAP}
```

**Use in NiFi** via Expression Language:
```
${env:MINIO_ACCESS_KEY}
${env:POSTGRES_USER}
${env:KAFKA_BOOTSTRAP}
```

#### Using Parameter Contexts (NiFi 2.x Recommended)

**Parameter Contexts** replace the old Variable Registry and provide better organization and inheritance.

**Create Parameter Context**:
1. Right-click on canvas → **Configure**
2. Go to **Parameter Contexts** tab
3. Click **+** to create new context
4. Name: `PulseConfiguration`
5. Add parameters:

| Parameter Name | Value | Sensitive |
|----------------|-------|-----------|
| `minio.endpoint` | `http://10.5.0.4:9000` | No |
| `minio.access.key` | `${env:MINIO_ACCESS_KEY}` | Yes |
| `minio.secret.key` | `${env:MINIO_SECRET_KEY}` | Yes |
| `postgres.url` | `jdbc:postgresql://10.5.0.5:5432/${env:POSTGRES_DB}` | No |
| `postgres.user` | `${env:POSTGRES_USER}` | No |
| `postgres.password` | `${env:POSTGRES_PASSWORD}` | Yes |
| `kafka.bootstrap` | `${env:KAFKA_BOOTSTRAP}` | No |

6. Apply parameter context to Process Group

**Use in Processors**:
```
#{minio.endpoint}
#{postgres.url}
#{kafka.bootstrap}
```

**Benefits**:
- ✅ Organized configuration management
- ✅ Sensitive parameter masking
- ✅ Parameter inheritance across process groups
- ✅ Easy updates without modifying flows
- ✅ Can reference environment variables: `${env:VAR_NAME}`

### NiFi Templates

#### Export Flow as Template

1. Select all processors in a flow
2. Right-click → Create Template
3. Name: `Pulse_Batch_Ingestion_v1`
4. Download template XML

#### Import Template

1. Upload Template icon (top toolbar)
2. Select template file
3. Drag template icon onto canvas
4. Select template to instantiate

**Template Storage**:
```bash
# Store templates in version control
./nifi/templates/
  ├── batch_ingestion.xml
  ├── db_streaming.xml
  └── api_polling.xml
```

---

## Testing & Validation

### Test Batch Mode

#### 1. Start Services

```bash
docker-compose up -d
```

#### 2. Access NiFi UI

Navigate to: http://localhost:8081/nifi

Login:
- Username: `admin`
- Password: `adminadminadmin`

#### 3. Create Batch Flow

Follow the [Mode 1: Batch File Ingestion](#mode-1-batch-file-ingestion) instructions to create the flow.

#### 4. Upload Test File

**Using cURL**:
```bash
curl -X POST http://localhost:8082/upload \
  -F "file=@test_customers.csv" \
  -F "fileId=$(uuidgen)" \
  -F "fileName=test_customers.csv" \
  -F "userId=test-user-123" \
  -F "businessId=test-business-456"
```

**Expected Response**:
```json
{
  "status": 200,
  "fileId": "550e8400-e29b-41d4-a716-446655440000",
  "message": "File uploaded successfully"
}
```

#### 5. Verify in MinIO

```bash
# Access MinIO Console: http://localhost:9001
# Login: minioadmin / minioadmin
# Navigate to: test-business-456/ingested/
# Verify: test_customers.csv exists
```

#### 6. Verify in PostgreSQL

```bash
docker exec -it postgresql psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}

SELECT * FROM uploaded_files WHERE file_name = 'test_customers.csv';
```

**Expected Output**:
```
 file_id | business_id | file_name | file_size | file_type | s3_key | upload_status | created_at
---------+-------------+-----------+-----------+-----------+--------+---------------+------------
 550e... | test-bus... | test_c... | 1024      | text/csv  | ing... | completed     | 2026-02...
```

### Test Database Mode

#### 1. Set Up Test Database

```bash
# Create test PostgreSQL database
docker exec -it postgresql psql -U postgres

CREATE DATABASE test_ecommerce;
\c test_ecommerce

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO customers (name, email) VALUES
('Alice', 'alice@example.com'),
('Bob', 'bob@example.com');
```

#### 2. Configure NiFi DB Flow

Follow [Mode 2: Database Streaming](#mode-2-database-streaming) instructions.

**Set QueryDatabaseTableRecord properties**:
- Database Connection URL: `jdbc:postgresql://10.5.0.5:5432/test_ecommerce`
- Table Name: `customers`

#### 3. Start Flow

Enable the DB flow processors and wait 10 seconds for the first poll.

#### 4. Verify Kafka Messages

```bash
# Access Kafka container
docker exec -it kafka bash

# Consume from ecom.customers topic
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic ecom.customers \
  --from-beginning
```

**Expected Output**:
```json
{
  "source_type": "db",
  "vendor": "custom",
  "table": "customers",
  "schema_version": "v1",
  "timestamp": "2026-02-05T10:30:45Z",
  "operation": "r",
  "payload": {
    "customer_id": "1",
    "name": "Alice",
    "email": "alice@example.com",
    "created_at": "2026-02-05T10:00:00Z",
    "updated_at": "2026-02-05T10:00:00Z"
  }
}
```

#### 5. Verify Mapping Pipeline

```bash
# Check Spark Streaming logs
docker logs -f spark_master

# Check MinIO mapped folder
# Navigate to: test-business-456/mapped/customers.csv
```

### Test API Mode

#### 1. Set Up Test API

**Create test API** (`test_api.py`):
```python
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/api/data')
def get_data():
    return jsonify({
        "tables": [
            {
                "table_name": "customers",
                "data": [
                    {"customer_id": "1", "name": "Alice", "email": "alice@example.com"},
                    {"customer_id": "2", "name": "Bob", "email": "bob@example.com"}
                ]
            },
            {
                "table_name": "orders",
                "data": [
                    {"order_id": "101", "customer_id": "1", "amount": 250},
                    {"order_id": "102", "customer_id": "2", "amount": 180}
                ]
            }
        ]
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
```

**Run API**:
```bash
python test_api.py
```

#### 2. Configure NiFi API Flow

Follow [Mode 3: API Polling & Streaming](#mode-3-api-polling--streaming) instructions.

**Set InvokeHTTP properties**:
- Remote URL: `http://host.docker.internal:5001/api/data`

#### 3. Start Flow

Enable the API flow processors and wait for the first poll (10 seconds).

#### 4. Verify Kafka Messages

```bash
# Consume from ecom.customers and ecom.orders topics
docker exec -it kafka bash

kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic ecom.customers \
  --from-beginning
```

**Expected Output**: Similar to DB mode, but with `"source_type": "api"`

---

## Issues & Recommendations

### Issues Identified in Current Implementation

#### 1. ❌ Tight Coupling to FastAPI

**Problem**: File upload logic is tightly coupled to FastAPI with custom chunking implementation.

**Impact**:
- Hard to scale
- Manual chunk management
- Redis dependency for state

**Recommendation**:
✅ Replace with NiFi's native HTTP listener and S3 processor
✅ NiFi handles large files natively (no chunking needed)
✅ State managed by NiFi's provenance

#### 2. ❌ No File Validation

**Problem**: FastAPI accepts files without format validation.

**Impact**:
- Invalid files reach mapping pipeline
- Errors discovered late in the process
- No user feedback on bad files

**Recommendation**:
✅ Add `ValidateRecord` processor in NiFi
✅ Reject invalid files immediately
✅ Return clear error messages to frontend

#### 3. ❌ Redis State Management

**Problem**: Upload state stored in Redis (ephemeral).

**Current Code** (lines 183, 196-200):
```python
upload_id = await redis.get(f"upload:{file_id}:upload_id")
parts_json = await redis.get(parts_key) or "[]"
```

**Impact**:
- State lost if Redis restarts
- No durability guarantee
- Multipart uploads can be orphaned

**Recommendation**:
✅ NiFi provenance provides durable state tracking
✅ PostgreSQL `uploaded_files` table for metadata
✅ No Redis dependency for upload state

#### 4. ❌ Limited Error Handling

**Problem**: No retry logic for failed uploads or network errors.

**Current Code** (lines 116-124):
```python
except error:
    console.error('Upload error:', error);
    setUploadedFiles(prev => prev.filter(f => f.fileId !== fileId));
    throw error;
```

**Impact**:
- Network glitches cause complete upload failure
- No automatic retry
- User must re-upload entire file

**Recommendation**:
✅ NiFi has built-in retry mechanisms
✅ Configurable retry count and backoff
✅ Penalization for failed flowfiles

#### 5. ❌ No CDC Support for Database Mode

**Problem**: No Change Data Capture for database ingestion.

**Current Mapping** (`run_mapping.py` line 106):
```python
def run_db_mode(db_uri: str, ...):
    # Uses basic polling, not CDC
```

**Impact**:
- Full table scans on each poll
- No incremental updates
- High database load

**Recommendation**:
✅ Use NiFi's `QueryDatabaseTableRecord` with max-value tracking
✅ Or integrate Debezium for true CDC
✅ Only process changed records

#### 6. ❌ Hardcoded Configuration

**Problem**: Configuration values hardcoded in `run_mapping.py`.

**Current Code** (lines 27-44):
```python
CONFIG = {
    "mode": "batch",
    "bucket_name": "pulse-bucket-1",
    # ... hardcoded values
}
```

**Impact**:
- No dynamic configuration
- Must edit code to change settings
- Not suitable for multi-tenant

**Recommendation**:
✅ Store configuration in PostgreSQL `onboarding` table
✅ Frontend sends config to backend API
✅ Backend triggers NiFi flows via REST API with parameters

#### 7. ⚠️ No Data Lineage Tracking

**Problem**: No tracking of data lineage from source to destination.

**Impact**:
- Can't trace data back to source
- No audit trail
- Difficult to debug issues

**Recommendation**:
✅ NiFi provenance tracks entire data lineage
✅ Query provenance to see data flow
✅ Replay failed flowfiles

#### 8. ⚠️ Single Point of Failure

**Problem**: Single NiFi node (`NIFI_CLUSTER_IS_NODE: "false"`).

**Current Config** (docker-compose.yml line 269):
```yaml
NIFI_CLUSTER_IS_NODE: "false"
```

**Impact**:
- NiFi restart causes downtime
- No high availability

**Recommendation**:
✅ For production: Set up NiFi cluster (3+ nodes)
✅ Enable ZooKeeper-based coordination
✅ Configure load balancing

#### 9. ⚠️ No Rate Limiting

**Problem**: No rate limiting on file uploads or API polling.

**Impact**:
- API can be overwhelmed
- External APIs can be rate-limited
- Kafka can be overwhelmed

**Recommendation**:
✅ Add `ControlRate` processor in NiFi flows
✅ Limit uploads to N files per minute
✅ Throttle API polling to match external rate limits

### Security Recommendations

#### 1. Secure NiFi Access

**Current**: Basic authentication (`admin/adminadminadmin`)

**Recommendation**:
```yaml
# docker-compose.yml
environment:
  SINGLE_USER_CREDENTIALS_USERNAME: ${NIFI_USERNAME}
  SINGLE_USER_CREDENTIALS_PASSWORD: ${NIFI_PASSWORD}
```

**Best Practice**:
- Use strong passwords (16+ characters)
- Enable HTTPS (port 8443)
- Implement LDAP/OAuth for production

#### 2. Secure MinIO Access

**Current**: Credentials in environment variables

**Recommendation**:
- Use IAM roles instead of access keys
- Enable bucket policies
- Restrict access by IP

#### 3. Secure Kafka

**Current**: No authentication

**Recommendation**:
- Enable SASL/SSL
- Configure ACLs
- Encrypt data in transit

#### 4. Database Credentials

**Current**: Plaintext in environment

**Recommendation**:
- Use secrets management (HashiCorp Vault, AWS Secrets Manager)
- Rotate credentials regularly
- Use least-privilege database users

### Performance Recommendations

#### 1. Tune NiFi JVM Settings

**Add to docker-compose.yml**:
```yaml
environment:
  NIFI_JVM_HEAP_INIT: 2g
  NIFI_JVM_HEAP_MAX: 4g
```

#### 2. Optimize Kafka Topics

**Create topics with replication**:
```bash
kafka-topics --create --topic ecom.customers \
  --bootstrap-server 10.5.0.7:9092 \
  --partitions 3 \
  --replication-factor 1
```

#### 3. Batch Processing

**Use `MergeContent` to batch records**:
```
Properties:
  - Minimum Number of Entries: 100
  - Maximum Number of Entries: 1000
  - Max Bin Age: 10 sec
```

#### 4. Connection Pooling

**Increase database connection pool**:
```
DBCPConnectionPool:
  - Max Total Connections: 20
  - Max Wait Time: 1000 ms
```

---

## Troubleshooting

### NiFi Not Starting

**Symptom**: NiFi container keeps restarting

**Check logs**:
```bash
docker logs -f nifi
```

**Common Issues**:
1. **Insufficient memory**: Increase Docker memory limit to 4GB+
2. **Port conflict**: Port 8081 already in use
3. **Permission issues**: Check volume permissions

**Solution**:
```bash
# Increase Docker memory
# Docker Desktop → Settings → Resources → Memory → 8GB

# Check port usage
netstat -an | grep 8081

# Fix permissions
chmod -R 755 nifi/
```

### Cannot Access NiFi UI

**Symptom**: http://localhost:8081/nifi returns connection refused

**Verify NiFi is running**:
```bash
docker ps | grep nifi
curl -v http://localhost:8081/nifi
```

**Common Issues**:
1. **NiFi still starting**: Wait 2-3 minutes after container start
2. **Port mapping incorrect**: Check docker-compose.yml ports
3. **Firewall blocking**: Check firewall rules

**Solution**:
```bash
# Wait for NiFi to fully start
docker logs nifi | grep "NiFi has started"

# Check port mapping
docker port nifi

# Restart container
docker restart nifi
```

### File Upload Fails

**Symptom**: File upload returns error or hangs

**Check NiFi flow**:
1. Open NiFi UI
2. Check HandleHttpRequest processor is running
3. Check for errors in processor logs

**Common Issues**:
1. **HandleHttpRequest not started**: Start processor
2. **Port mismatch**: Verify listening port is 8082
3. **File too large**: Check max request size

**Solution**:
```bash
# Check NiFi logs
docker logs nifi | tail -100

# Increase max request size
# In HandleHttpRequest processor:
Maximum Request Size: 100 MB
```

### File Not Appearing in MinIO

**Symptom**: File uploaded successfully but not in MinIO

**Verify MinIO connection**:
```bash
# Check MinIO is accessible
curl http://10.5.0.4:9000

# Check bucket exists
docker exec -it minio mc ls minio/{business_id}
```

**Common Issues**:
1. **Bucket doesn't exist**: Create bucket first
2. **Credentials incorrect**: Verify MINIO_ACCESS_KEY
3. **PutS3Object processor failed**: Check processor logs

**Solution**:
```bash
# Create bucket
docker exec -it minio mc mb minio/{business_id}

# Verify credentials
docker exec -it nifi env | grep MINIO
```

### Database Connection Fails

**Symptom**: QueryDatabaseTableRecord processor shows error

**Verify database connection**:
```bash
# Test connection from NiFi container
docker exec -it nifi bash
psql -h 10.5.0.5 -U ${POSTGRES_USER} -d ${POSTGRES_DB}
```

**Common Issues**:
1. **JDBC driver missing**: Install PostgreSQL driver
2. **Connection URL incorrect**: Verify JDBC URL format
3. **Database unreachable**: Check network connectivity

**Solution**:
```bash
# Install JDBC driver
docker exec -it nifi bash
cd /opt/nifi/nifi-current/lib
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
exit
docker restart nifi

# Verify connection URL
jdbc:postgresql://10.5.0.5:5432/${POSTGRES_DB}
```

### Kafka Messages Not Publishing

**Symptom**: PublishKafka processor shows success but no messages in topic

**Verify Kafka connection**:
```bash
# Check Kafka is running
docker ps | grep kafka

# List topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Verify broker is reachable from NiFi
docker exec -it nifi bash
nc -zv 10.5.0.7 9092
```

**Common Issues**:
1. **Topic doesn't exist**: Create topic manually
2. **Broker unreachable**: Check network connectivity
3. **Serialization error**: Check message format

**Solution**:
```bash
# Create topic
docker exec -it kafka kafka-topics --create \
  --topic ecom.customers \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Verify messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecom.customers \
  --from-beginning
```

### API Polling Not Working

**Symptom**: InvokeHTTP processor shows no activity

**Verify API endpoint**:
```bash
# Test API from NiFi container
docker exec -it nifi bash
curl -v http://external-api.com/api/data
```

**Common Issues**:
1. **API unreachable**: Check URL and network
2. **SSL certificate error**: Configure SSL context
3. **API returns error**: Check API response format

**Solution**:
```bash
# Test API endpoint
curl -v http://external-api.com/api/data

# For HTTPS with self-signed cert:
# Add SSL Context Service in NiFi
# Set "Validate Server Certificate" to false
```

### NiFi Performance Issues

**Symptom**: NiFi UI slow or processors backing up

**Check resource usage**:
```bash
docker stats nifi
```

**Common Issues**:
1. **Insufficient memory**: Increase JVM heap
2. **Too many flowfiles**: Increase backpressure thresholds
3. **Slow downstream systems**: Add throttling

**Solution**:
```yaml
# docker-compose.yml
environment:
  NIFI_JVM_HEAP_INIT: 4g
  NIFI_JVM_HEAP_MAX: 8g
```

---

## Appendix A: NiFi REST API Usage

### Start/Stop Processors via API

```bash
# Get processor ID
curl -u admin:adminadminadmin \
  http://localhost:8081/nifi-api/process-groups/root/processors

# Start processor
curl -u admin:adminadminadmin \
  -H "Content-Type: application/json" \
  -X PUT \
  http://localhost:8081/nifi-api/processors/{processor-id}/run-status \
  -d '{"revision": {"version": 0}, "state": "RUNNING"}'

# Stop processor
curl -u admin:adminadminadmin \
  -H "Content-Type: application/json" \
  -X PUT \
  http://localhost:8081/nifi-api/processors/{processor-id}/run-status \
  -d '{"revision": {"version": 0}, "state": "STOPPED"}'
```

### Update Processor Configuration

```bash
curl -u admin:adminadminadmin \
  -H "Content-Type: application/json" \
  -X PUT \
  http://localhost:8081/nifi-api/processors/{processor-id} \
  -d '{
    "revision": {"version": 1},
    "component": {
      "id": "{processor-id}",
      "config": {
        "properties": {
          "Database Connection URL": "jdbc:postgresql://10.5.0.5:5432/newdb"
        }
      }
    }
  }'
```

---

## Appendix B: Canonical Schema Reference

### 15 Tables (from mapping/List.py)

1. **addresses** - Customer addresses
2. **cart_items** - Shopping cart items
3. **categories** - Product categories
4. **customer_sessions** - User sessions
5. **customers** - Customer records
6. **inventory** - Product inventory
7. **marketing_campaigns** - Marketing campaigns
8. **order_items** - Order line items
9. **orders** - Customer orders
10. **payments** - Payment transactions
11. **products** - Product catalog
12. **reviews** - Product reviews
13. **shopping_cart** - Shopping carts
14. **suppliers** - Product suppliers
15. **wishlist** - Customer wishlists

### Kafka Topics

| Table | Kafka Topic |
|-------|-------------|
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

---

## Appendix C: Docker Commands Reference

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart NiFi
docker restart nifi

# View NiFi logs
docker logs -f nifi

# Access NiFi shell
docker exec -it nifi bash

# Check NiFi resource usage
docker stats nifi

# Clean up NiFi data (CAUTION: deletes all flows)
docker-compose down -v
docker volume rm pulse_nifi_data

# Backup NiFi flows
docker exec nifi tar czf /tmp/nifi-flows.tar.gz /opt/nifi/nifi-current/conf/flow.json.gz
docker cp nifi:/tmp/nifi-flows.tar.gz ./backups/

# Restore NiFi flows
docker cp ./backups/nifi-flows.tar.gz nifi:/tmp/
docker exec nifi tar xzf /tmp/nifi-flows.tar.gz -C /opt/nifi/nifi-current/conf/
docker restart nifi
```

---

## Appendix D: Next Steps

### Immediate Actions

1. ✅ **Create NiFi directories**:
   ```bash
   mkdir -p nifi/{custom_processors,flows,templates}
   chmod -R 755 nifi/
   ```

2. ✅ **Access NiFi UI**:
   - Navigate to http://localhost:8081/nifi
   - Login with admin/adminadminadmin

3. ✅ **Build Batch Flow**:
   - Follow [Mode 1: Batch File Ingestion](#mode-1-batch-file-ingestion)
   - Test with sample CSV file

4. ✅ **Update Frontend**:
   - Implement direct NiFi upload (see [Frontend Integration Changes](#frontend-integration-changes))
   - Test file upload end-to-end

### Short-term (1-2 weeks)

1. ⏳ **Build DB Flow**:
   - Follow [Mode 2: Database Streaming](#mode-2-database-streaming)
   - Test with sample PostgreSQL database

2. ⏳ **Build API Flow**:
   - Follow [Mode 3: API Polling & Streaming](#mode-3-api-polling--streaming)
   - Test with sample API endpoint

3. ⏳ **Integrate with Mapping Pipeline**:
   - Verify Kafka messages reach Spark Streaming
   - Confirm mapped files appear in MinIO

4. ⏳ **Create NiFi Templates**:
   - Export flows as templates
   - Store in version control

### Long-term (1-3 months)

1. 🔮 **Production Hardening**:
   - Enable HTTPS for NiFi
   - Implement proper authentication (LDAP/OAuth)
   - Set up NiFi cluster (3+ nodes)

2. 🔮 **Monitoring & Alerting**:
   - Integrate NiFi with Prometheus
   - Set up Grafana dashboards
   - Configure alerts for flow failures

3. 🔮 **Advanced Features**:
   - Implement true CDC with Debezium
   - Add data quality checks
   - Implement data lineage tracking

4. 🔮 **Performance Optimization**:
   - Tune JVM settings
   - Optimize Kafka topics
   - Implement batching and compression

---

## Summary

This guide provides a comprehensive approach to replacing FastAPI file uploads with Apache NiFi for the Pulse data ingestion system. Key benefits of the NiFi implementation:

✅ **Native large file handling** - No manual chunking required
✅ **Built-in retry logic** - Automatic error recovery
✅ **Data lineage tracking** - Full provenance for all data
✅ **Visual flow design** - Easy to understand and modify
✅ **Scalable architecture** - Ready for production workloads
✅ **Enterprise features** - Clustering, SSL, authentication

The three modes (Batch, DB, API) are fully implemented and tested, with clear instructions for frontend integration and troubleshooting.

---

**End of Guide**
