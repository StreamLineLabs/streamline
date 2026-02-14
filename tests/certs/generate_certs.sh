#!/bin/bash
# Generate test certificates for TLS testing (compatible with rustls)

set -e

echo "Generating test certificates..."

# Generate CA private key
openssl genrsa -out ca-key.pem 2048

# Generate CA certificate (v3 certificate)
openssl req -new -x509 -days 365 -key ca-key.pem -out ca-cert.pem \
    -subj "/C=US/ST=Test/L=Test/O=Streamline Test CA/CN=Test CA" \
    -extensions v3_ca \
    -config <(cat <<-EOC
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca

[req_distinguished_name]

[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
EOC
)

# Generate server private key (explicitly in PKCS#8 format for rustls)
openssl genrsa -out server-key.pem 2048

# Generate server certificate signing request
openssl req -new -key server-key.pem -out server.csr \
    -subj "/C=US/ST=Test/L=Test/O=Streamline/CN=localhost"

# Create v3 extension file for server cert
cat > server_ext.cnf << 'EOC'
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = 127.0.0.1
IP.1 = 127.0.0.1
EOC

# Sign server certificate with CA
openssl x509 -req -days 365 -in server.csr \
    -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -out server-cert.pem -extfile server_ext.cnf

# Generate client private key (for mTLS)
openssl genrsa -out client-key.pem 2048

# Generate client certificate signing request
openssl req -new -key client-key.pem -out client.csr \
    -subj "/C=US/ST=Test/L=Test/O=Streamline Client/CN=test-client"

# Create v3 extension file for client cert
cat > client_ext.cnf << 'EOC'
basicConstraints = CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = clientAuth
EOC

# Sign client certificate with CA
openssl x509 -req -days 365 -in client.csr \
    -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -out client-cert.pem -extfile client_ext.cnf

# Clean up temporary files
rm server.csr client.csr server_ext.cnf client_ext.cnf

echo "Test certificates generated successfully!"
echo "Files created:"
echo "  ca-cert.pem, ca-key.pem - Certificate Authority"
echo "  server-cert.pem, server-key.pem - Server certificate and key"
echo "  client-cert.pem, client-key.pem - Client certificate and key (for mTLS)"
