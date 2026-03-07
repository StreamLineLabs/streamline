# TLS Configuration

Streamline supports TLS for encrypting client-broker communication.

## Quick Start

```bash
streamline --tls-cert-file server.pem --tls-key-file server-key.pem
```

## Mutual TLS (mTLS)

For environments requiring client authentication:

```bash
streamline \
  --tls-cert-file server.pem \
  --tls-key-file server-key.pem \
  --tls-ca-file ca.pem \
  --tls-require-client-auth
```

## Generating Test Certificates

```bash
# Generate CA
openssl req -x509 -newkey rsa:4096 -days 365 -nodes \
  -keyout ca-key.pem -out ca.pem -subj "/CN=Streamline CA"

# Generate server certificate
openssl req -newkey rsa:4096 -nodes \
  -keyout server-key.pem -out server-req.pem \
  -subj "/CN=localhost"

openssl x509 -req -in server-req.pem \
  -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server.pem -days 365
```
