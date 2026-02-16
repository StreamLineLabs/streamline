#!/usr/bin/env bash
# Streamline TLS Setup Example
#
# This script demonstrates how to set up TLS encryption for Streamline.
# It generates self-signed certificates for testing purposes.
#
# For production, use certificates from a trusted Certificate Authority (CA)
# or your organization's PKI infrastructure.
#
# Usage:
#   ./examples/tls_setup.sh generate  # Generate test certificates
#   ./examples/tls_setup.sh start     # Start server with TLS
#   ./examples/tls_setup.sh test      # Test TLS connection
#   ./examples/tls_setup.sh cleanup   # Remove generated certificates

set -e

# Configuration
STREAMLINE_BIN="${STREAMLINE_BIN:-./target/release/streamline}"
CERTS_DIR="${CERTS_DIR:-/tmp/streamline-tls}"
DATA_DIR="${DATA_DIR:-/tmp/streamline-tls-data}"

# Certificate parameters
DAYS_VALID=365
KEY_SIZE=4096
COMMON_NAME="localhost"

generate_certs() {
    echo "Generating TLS certificates..."
    mkdir -p "$CERTS_DIR"
    cd "$CERTS_DIR"

    # Generate CA key and certificate
    echo "1. Generating CA certificate..."
    openssl genrsa -out ca.key $KEY_SIZE 2>/dev/null
    openssl req -new -x509 -days $DAYS_VALID -key ca.key -out ca.crt \
        -subj "/CN=Streamline Test CA/O=Streamline/C=US" 2>/dev/null

    # Generate server key
    echo "2. Generating server key..."
    openssl genrsa -out server.key $KEY_SIZE 2>/dev/null

    # Generate server CSR
    echo "3. Generating server certificate..."
    cat > server.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = streamline.local
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

    openssl req -new -key server.key -out server.csr \
        -subj "/CN=$COMMON_NAME/O=Streamline/C=US" 2>/dev/null

    openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
        -CAcreateserial -out server.crt -days $DAYS_VALID \
        -extfile server.ext 2>/dev/null

    # Generate client key and certificate (for mTLS)
    echo "4. Generating client certificate (for mTLS)..."
    openssl genrsa -out client.key $KEY_SIZE 2>/dev/null

    openssl req -new -key client.key -out client.csr \
        -subj "/CN=streamline-client/O=Streamline/C=US" 2>/dev/null

    cat > client.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

    openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key \
        -CAcreateserial -out client.crt -days $DAYS_VALID \
        -extfile client.ext 2>/dev/null

    # Cleanup CSR files
    rm -f server.csr client.csr server.ext client.ext

    # Set appropriate permissions
    chmod 600 *.key
    chmod 644 *.crt

    echo ""
    echo "Certificates generated in: $CERTS_DIR"
    echo ""
    echo "Files:"
    ls -la "$CERTS_DIR"
    echo ""
    echo "Certificate details:"
    echo "  CA:     $CERTS_DIR/ca.crt (CA certificate)"
    echo "  Server: $CERTS_DIR/server.crt + server.key"
    echo "  Client: $CERTS_DIR/client.crt + client.key (for mTLS)"
}

start_server() {
    echo "Starting Streamline with TLS..."

    if [ ! -f "$CERTS_DIR/server.crt" ]; then
        echo "Error: Certificates not found. Run '$0 generate' first."
        exit 1
    fi

    mkdir -p "$DATA_DIR"

    echo ""
    echo "Starting server..."
    echo "  TLS enabled: true"
    echo "  Certificate: $CERTS_DIR/server.crt"
    echo "  Key: $CERTS_DIR/server.key"
    echo "  Min TLS version: 1.2"
    echo ""

    $STREAMLINE_BIN \
        --tls-enabled \
        --tls-cert "$CERTS_DIR/server.crt" \
        --tls-key "$CERTS_DIR/server.key" \
        --tls-min-version 1.2 \
        --data-dir "$DATA_DIR" \
        --log-level info &

    SERVER_PID=$!
    echo $SERVER_PID > "$DATA_DIR/streamline.pid"

    echo "Server started (PID: $SERVER_PID)"
    echo ""
    echo "Connect using TLS:"
    echo "  # Using Kafka clients (add these to your client config):"
    echo "  security.protocol=SSL"
    echo "  ssl.truststore.location=/path/to/truststore.jks"
    echo "  ssl.truststore.password=password"
    echo ""
    echo "  # Or using OpenSSL to test:"
    echo "  openssl s_client -connect localhost:9092 -CAfile $CERTS_DIR/ca.crt"
}

start_server_mtls() {
    echo "Starting Streamline with mTLS (mutual TLS)..."

    if [ ! -f "$CERTS_DIR/server.crt" ]; then
        echo "Error: Certificates not found. Run '$0 generate' first."
        exit 1
    fi

    mkdir -p "$DATA_DIR"

    echo ""
    echo "Starting server with mTLS..."
    echo "  TLS enabled: true"
    echo "  Client cert required: true"
    echo "  CA cert: $CERTS_DIR/ca.crt"
    echo ""

    $STREAMLINE_BIN \
        --tls-enabled \
        --tls-cert "$CERTS_DIR/server.crt" \
        --tls-key "$CERTS_DIR/server.key" \
        --tls-require-client-cert \
        --tls-ca-cert "$CERTS_DIR/ca.crt" \
        --tls-min-version 1.2 \
        --data-dir "$DATA_DIR" \
        --log-level info &

    SERVER_PID=$!
    echo $SERVER_PID > "$DATA_DIR/streamline.pid"

    echo "Server started (PID: $SERVER_PID)"
    echo ""
    echo "Connect using mTLS (client certificate required):"
    echo "  openssl s_client -connect localhost:9092 \\"
    echo "    -CAfile $CERTS_DIR/ca.crt \\"
    echo "    -cert $CERTS_DIR/client.crt \\"
    echo "    -key $CERTS_DIR/client.key"
}

test_connection() {
    echo "Testing TLS connection..."

    if [ ! -f "$CERTS_DIR/ca.crt" ]; then
        echo "Error: Certificates not found. Run '$0 generate' first."
        exit 1
    fi

    echo ""
    echo "Connecting to localhost:9092 with TLS..."
    echo ""

    # Use openssl to test the connection
    echo "Q" | openssl s_client -connect localhost:9092 -CAfile "$CERTS_DIR/ca.crt" 2>/dev/null | head -30

    echo ""
    echo "Connection test complete."
}

stop_server() {
    PID_FILE="$DATA_DIR/streamline.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "Stopping server (PID: $PID)..."
            kill "$PID"
            rm -f "$PID_FILE"
        fi
    fi
}

cleanup() {
    echo "Cleaning up..."
    stop_server
    rm -rf "$CERTS_DIR"
    rm -rf "$DATA_DIR"
    echo "Done."
}

case "${1:-}" in
    generate)
        generate_certs
        ;;
    start)
        start_server
        ;;
    start-mtls)
        start_server_mtls
        ;;
    test)
        test_connection
        ;;
    stop)
        stop_server
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "Usage: $0 {generate|start|start-mtls|test|stop|cleanup}"
        echo ""
        echo "Commands:"
        echo "  generate    Generate self-signed certificates for testing"
        echo "  start       Start server with TLS (no client cert required)"
        echo "  start-mtls  Start server with mTLS (client cert required)"
        echo "  test        Test TLS connection"
        echo "  stop        Stop the server"
        echo "  cleanup     Remove all generated files"
        exit 1
        ;;
esac
