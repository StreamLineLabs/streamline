# GDPR Compliance Guide

## Overview

Streamline provides several features to support GDPR compliance for organizations processing personal data through streaming pipelines.

## Data Subject Rights

### Right to Erasure (Article 17)

Streamline's data lineage module (`src/lineage/gdpr.rs`) provides tools for implementing the right to erasure:

```bash
# Identify all topics containing data for a specific subject
streamline-cli lineage trace --subject-id "user-123"

# Delete records matching a subject (compaction-based)
streamline-cli topics compact my-topic --delete-key "user-123"
```

**Limitations:**
- Segment-based storage means immediate physical deletion is not possible
- Records are removed during compaction cycles
- Encrypted data can be made inaccessible via key deletion (crypto-shredding)

### Right to Access (Article 15)

```bash
# Export all data for a subject
streamline-cli consume events --grep-key "user-123" --format json > subject-data.json
```

### Right to Portability (Article 20)

```bash
# Export in standard format
streamline-cli topics export events -o events.jsonl --filter-key "user-123"
```

## Data Processing Records

### What Streamline Tracks

| Data Point | Where | Retention |
|------------|-------|-----------|
| Message keys/values | Topic segments | Configurable retention |
| Consumer offsets | Internal topic | Indefinite |
| Authentication events | Audit log | Configurable |
| Client IP addresses | Connection log | Session duration |

### Data Residency

Streamline supports data residency requirements through:

1. **Single-region deployment**: Data stays in one region
2. **Rack-aware replication**: Control which racks/zones hold replicas
3. **Topic-level storage config**: Different retention per topic
4. **Tiered storage**: Control where cold data is archived (S3 region, Azure region, etc.)

## Encryption

| Layer | Mechanism | Status |
|-------|-----------|--------|
| In transit | TLS 1.2/1.3 | ✅ Production |
| At rest | AES-256-GCM | ✅ Production |
| Key management | File-based keys | ⚠️ No rotation yet |

## Recommendations for GDPR Compliance

1. **Enable encryption** at rest and in transit for all topics containing personal data
2. **Enable audit logging** to track access to personal data topics
3. **Set retention policies** on topics with personal data (align with data processing purposes)
4. **Use ACLs** to restrict access to personal data topics to authorized services only
5. **Implement crypto-shredding** for erasure: encrypt per-subject, delete key on erasure request
6. **Document your data flows** using Streamline's lineage tracking
