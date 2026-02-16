#!/usr/bin/env bash
# Streamline Authentication Setup Example
#
# This script demonstrates how to set up SASL authentication for Streamline.
# It supports PLAIN and SCRAM-SHA-256/512 mechanisms.
#
# Prerequisites:
#   - Build Streamline with auth feature:
#     cargo build --release --features auth
#
# Usage:
#   ./examples/auth_setup.sh setup     # Create users file and start server
#   ./examples/auth_setup.sh add-user  # Add a new user
#   ./examples/auth_setup.sh test      # Test authentication
#   ./examples/auth_setup.sh cleanup   # Remove all files

set -e

# Configuration
STREAMLINE_BIN="${STREAMLINE_BIN:-./target/release/streamline}"
STREAMLINE_CLI="${STREAMLINE_CLI:-./target/release/streamline-cli}"
DATA_DIR="${DATA_DIR:-/tmp/streamline-auth}"
USERS_FILE="${DATA_DIR}/users.yaml"

setup() {
    echo "Setting up Streamline with authentication..."
    mkdir -p "$DATA_DIR"

    # Create users file with default users
    echo "Creating users file..."
    cat > "$USERS_FILE" << 'EOF'
# Streamline Users File
# Format: username -> password (hashed) and metadata
#
# Users can be added using:
#   streamline-cli users add --users-file users.yaml <username> -p <password>
#
# Password format: plain text (will be hashed on first use) or
# pre-hashed with argon2 or SCRAM-SHA-256/512

users:
  # Admin user - full access
  admin:
    password: "admin123"
    roles:
      - admin
    metadata:
      created_by: setup-script
      created_at: "2024-01-01T00:00:00Z"

  # Producer user - can produce to topics
  producer:
    password: "producer123"
    roles:
      - producer
    metadata:
      description: "Application producer service"

  # Consumer user - can consume from topics
  consumer:
    password: "consumer123"
    roles:
      - consumer
    metadata:
      description: "Application consumer service"

  # Read-only user - limited access
  readonly:
    password: "readonly123"
    roles:
      - reader
    metadata:
      description: "Monitoring and dashboards"
EOF

    chmod 600 "$USERS_FILE"
    echo "Users file created: $USERS_FILE"
    echo ""

    # Create ACL file
    ACL_FILE="${DATA_DIR}/acls.yaml"
    cat > "$ACL_FILE" << 'EOF'
# Streamline ACL Configuration
#
# Format: resource_type:resource_name -> principal:operation -> permission
#
# Resource types: topic, group, cluster, transactional_id
# Operations: read, write, create, delete, alter, describe, all
# Permissions: allow, deny

acls:
  # Admin has full access to everything
  - resource_type: cluster
    resource_name: "*"
    principal: "User:admin"
    operation: all
    permission: allow

  # Producers can write to any topic
  - resource_type: topic
    resource_name: "*"
    principal: "User:producer"
    operation: write
    permission: allow

  - resource_type: topic
    resource_name: "*"
    principal: "User:producer"
    operation: describe
    permission: allow

  # Consumers can read from any topic and manage their groups
  - resource_type: topic
    resource_name: "*"
    principal: "User:consumer"
    operation: read
    permission: allow

  - resource_type: group
    resource_name: "*"
    principal: "User:consumer"
    operation: read
    permission: allow

  - resource_type: group
    resource_name: "*"
    principal: "User:consumer"
    operation: describe
    permission: allow

  # Read-only can only describe/read
  - resource_type: topic
    resource_name: "*"
    principal: "User:readonly"
    operation: describe
    permission: allow

  - resource_type: topic
    resource_name: "*"
    principal: "User:readonly"
    operation: read
    permission: allow
EOF

    chmod 600 "$ACL_FILE"
    echo "ACL file created: $ACL_FILE"
    echo ""

    echo "Starting Streamline with authentication..."
    echo ""
    echo "Configuration:"
    echo "  Auth enabled: true"
    echo "  SASL mechanisms: PLAIN,SCRAM-SHA-256"
    echo "  Users file: $USERS_FILE"
    echo "  ACL enabled: true"
    echo "  ACL file: $ACL_FILE"
    echo ""

    $STREAMLINE_BIN \
        --auth-enabled \
        --auth-sasl-mechanisms "PLAIN,SCRAM-SHA-256" \
        --auth-users-file "$USERS_FILE" \
        --acl-enabled \
        --acl-file "$ACL_FILE" \
        --acl-super-users "User:admin" \
        --data-dir "$DATA_DIR" \
        --log-level info &

    SERVER_PID=$!
    echo $SERVER_PID > "$DATA_DIR/streamline.pid"

    echo "Server started (PID: $SERVER_PID)"
    echo ""
    echo "Test users:"
    echo "  admin/admin123     - Full access (super user)"
    echo "  producer/producer123 - Can produce to topics"
    echo "  consumer/consumer123 - Can consume from topics"
    echo "  readonly/readonly123 - Read-only access"
    echo ""
    echo "Connect with authentication:"
    echo "  # Using Kafka clients:"
    echo "  security.protocol=SASL_PLAINTEXT"
    echo "  sasl.mechanism=PLAIN"
    echo "  sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \\"
    echo "    username=\"admin\" password=\"admin123\";"
    echo ""
    echo "  # Using streamline-cli:"
    echo "  streamline-cli --broker localhost:9092 --username admin --password admin123 topics list"
}

add_user() {
    echo "Add a new user"
    echo ""

    read -p "Username: " username
    read -s -p "Password: " password
    echo ""
    read -p "Roles (comma-separated, e.g., producer,consumer): " roles

    echo ""
    echo "Adding user '$username'..."

    $STREAMLINE_CLI users add \
        --users-file "$USERS_FILE" \
        "$username" \
        -p "$password" \
        --roles "$roles"

    echo "User '$username' added successfully."
    echo ""
    echo "Don't forget to create ACLs for this user!"
}

test_auth() {
    echo "Testing authentication..."
    echo ""

    # Test admin user
    echo "1. Testing admin user..."
    if $STREAMLINE_CLI --broker localhost:9092 \
        --username admin --password admin123 \
        topics list 2>/dev/null; then
        echo "   Admin login: SUCCESS"
    else
        echo "   Admin login: FAILED"
    fi

    # Test invalid credentials
    echo ""
    echo "2. Testing invalid credentials..."
    if $STREAMLINE_CLI --broker localhost:9092 \
        --username admin --password wrongpassword \
        topics list 2>/dev/null; then
        echo "   Invalid password: UNEXPECTED SUCCESS (should fail)"
    else
        echo "   Invalid password: Correctly rejected"
    fi

    # Test producer user
    echo ""
    echo "3. Testing producer user..."
    if $STREAMLINE_CLI --broker localhost:9092 \
        --username producer --password producer123 \
        produce test-topic -m "Hello from producer" 2>/dev/null; then
        echo "   Producer can produce: SUCCESS"
    else
        echo "   Producer produce: FAILED (topic may not exist)"
    fi

    echo ""
    echo "Authentication tests complete."
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
    rm -rf "$DATA_DIR"
    echo "Done."
}

case "${1:-}" in
    setup)
        setup
        ;;
    add-user)
        add_user
        ;;
    test)
        test_auth
        ;;
    stop)
        stop_server
        ;;
    cleanup)
        cleanup
        ;;
    *)
        echo "Usage: $0 {setup|add-user|test|stop|cleanup}"
        echo ""
        echo "Commands:"
        echo "  setup      Create users/ACLs and start server with auth"
        echo "  add-user   Add a new user interactively"
        echo "  test       Test authentication with various users"
        echo "  stop       Stop the server"
        echo "  cleanup    Remove all generated files"
        exit 1
        ;;
esac
