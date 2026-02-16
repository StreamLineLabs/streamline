#!/usr/bin/env bash
# Streamline Backup and Restore Script
#
# This script provides backup and restore functionality for Streamline data.
# It handles topic data, consumer group offsets, and configuration files.
#
# Usage:
#   ./examples/backup_restore.sh backup [options]     # Create a backup
#   ./examples/backup_restore.sh restore [options]    # Restore from backup
#   ./examples/backup_restore.sh list                 # List available backups
#   ./examples/backup_restore.sh verify <backup>      # Verify backup integrity
#
# IMPORTANT: Always stop Streamline before performing backup/restore operations
# to ensure data consistency.

set -e

# Configuration
DATA_DIR="${DATA_DIR:-/var/lib/streamline}"
BACKUP_DIR="${BACKUP_DIR:-/var/backups/streamline}"
CONFIG_DIR="${CONFIG_DIR:-/etc/streamline}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS="${RETENTION_DAYS:-7}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_streamline_stopped() {
    if pgrep -x "streamline" > /dev/null 2>&1; then
        log_error "Streamline is still running. Please stop it first:"
        log_error "  sudo systemctl stop streamline"
        log_error "  # or: kill \$(pgrep streamline)"
        exit 1
    fi
}

backup() {
    local BACKUP_NAME="${1:-streamline_backup_${TIMESTAMP}}"
    local BACKUP_PATH="${BACKUP_DIR}/${BACKUP_NAME}"
    local ARCHIVE_PATH="${BACKUP_PATH}.tar.gz"

    log_info "Starting backup: ${BACKUP_NAME}"

    # Check if Streamline is stopped (recommended but not required for hot backup)
    if pgrep -x "streamline" > /dev/null 2>&1; then
        log_warn "Streamline is running. For consistent backups, consider stopping it first."
        log_warn "Continuing with hot backup (may have slight inconsistencies)..."
        sleep 2
    fi

    # Create backup directory
    mkdir -p "${BACKUP_PATH}"

    # Backup data directory
    if [ -d "${DATA_DIR}" ]; then
        log_info "Backing up data directory: ${DATA_DIR}"

        # Copy topics directory
        if [ -d "${DATA_DIR}/topics" ]; then
            log_info "  - Copying topics..."
            cp -r "${DATA_DIR}/topics" "${BACKUP_PATH}/"
        fi

        # Copy consumer groups (offsets)
        if [ -d "${DATA_DIR}/groups" ]; then
            log_info "  - Copying consumer groups..."
            cp -r "${DATA_DIR}/groups" "${BACKUP_PATH}/"
        fi

        # Copy WAL if exists
        if [ -d "${DATA_DIR}/wal" ]; then
            log_info "  - Copying WAL..."
            cp -r "${DATA_DIR}/wal" "${BACKUP_PATH}/"
        fi

        # Copy cluster state if exists
        if [ -d "${DATA_DIR}/cluster" ]; then
            log_info "  - Copying cluster state..."
            cp -r "${DATA_DIR}/cluster" "${BACKUP_PATH}/"
        fi

        # Copy any metadata files
        for f in "${DATA_DIR}"/*.json "${DATA_DIR}"/*.yaml; do
            if [ -f "$f" ]; then
                log_info "  - Copying $(basename "$f")..."
                cp "$f" "${BACKUP_PATH}/"
            fi
        done
    else
        log_warn "Data directory not found: ${DATA_DIR}"
    fi

    # Backup configuration
    if [ -d "${CONFIG_DIR}" ]; then
        log_info "Backing up configuration: ${CONFIG_DIR}"
        mkdir -p "${BACKUP_PATH}/config"
        cp -r "${CONFIG_DIR}"/* "${BACKUP_PATH}/config/" 2>/dev/null || true
    fi

    # Create manifest with metadata
    log_info "Creating backup manifest..."
    cat > "${BACKUP_PATH}/manifest.json" << EOF
{
    "backup_name": "${BACKUP_NAME}",
    "timestamp": "$(date -Iseconds)",
    "streamline_version": "$(streamline --version 2>/dev/null || echo 'unknown')",
    "data_dir": "${DATA_DIR}",
    "config_dir": "${CONFIG_DIR}",
    "hostname": "$(hostname)",
    "created_by": "$(whoami)"
}
EOF

    # Calculate checksums for integrity verification
    log_info "Calculating checksums..."
    find "${BACKUP_PATH}" -type f -exec sha256sum {} \; > "${BACKUP_PATH}/checksums.sha256"

    # Create compressed archive
    log_info "Creating compressed archive..."
    tar -czf "${ARCHIVE_PATH}" -C "${BACKUP_DIR}" "${BACKUP_NAME}"

    # Remove uncompressed backup directory
    rm -rf "${BACKUP_PATH}"

    # Calculate archive size
    local SIZE=$(du -h "${ARCHIVE_PATH}" | cut -f1)

    log_info "Backup complete!"
    log_info "  Archive: ${ARCHIVE_PATH}"
    log_info "  Size: ${SIZE}"

    # Cleanup old backups
    cleanup_old_backups
}

restore() {
    local ARCHIVE="${1}"
    local TARGET_DATA_DIR="${2:-${DATA_DIR}}"

    if [ -z "${ARCHIVE}" ]; then
        log_error "Usage: $0 restore <backup_archive> [target_data_dir]"
        exit 1
    fi

    if [ ! -f "${ARCHIVE}" ]; then
        # Try looking in backup directory
        if [ -f "${BACKUP_DIR}/${ARCHIVE}" ]; then
            ARCHIVE="${BACKUP_DIR}/${ARCHIVE}"
        elif [ -f "${BACKUP_DIR}/${ARCHIVE}.tar.gz" ]; then
            ARCHIVE="${BACKUP_DIR}/${ARCHIVE}.tar.gz"
        else
            log_error "Backup archive not found: ${ARCHIVE}"
            exit 1
        fi
    fi

    log_info "Starting restore from: ${ARCHIVE}"

    # Ensure Streamline is stopped
    check_streamline_stopped

    # Create temporary extraction directory
    local TEMP_DIR=$(mktemp -d)
    trap "rm -rf ${TEMP_DIR}" EXIT

    # Extract archive
    log_info "Extracting archive..."
    tar -xzf "${ARCHIVE}" -C "${TEMP_DIR}"

    # Find extracted directory
    local BACKUP_NAME=$(ls "${TEMP_DIR}")
    local BACKUP_PATH="${TEMP_DIR}/${BACKUP_NAME}"

    # Display manifest
    if [ -f "${BACKUP_PATH}/manifest.json" ]; then
        log_info "Backup manifest:"
        cat "${BACKUP_PATH}/manifest.json" | sed 's/^/  /'
    fi

    # Verify checksums
    if [ -f "${BACKUP_PATH}/checksums.sha256" ]; then
        log_info "Verifying checksums..."
        cd "${BACKUP_PATH}"
        if sha256sum -c checksums.sha256 > /dev/null 2>&1; then
            log_info "  Checksums verified successfully"
        else
            log_error "  Checksum verification failed!"
            log_error "  Backup may be corrupted. Aborting restore."
            exit 1
        fi
        cd - > /dev/null
    fi

    # Confirm restore
    log_warn "This will OVERWRITE data in: ${TARGET_DATA_DIR}"
    read -p "Are you sure you want to continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Restore cancelled."
        exit 0
    fi

    # Create backup of current data (safety measure)
    if [ -d "${TARGET_DATA_DIR}" ] && [ "$(ls -A ${TARGET_DATA_DIR} 2>/dev/null)" ]; then
        local SAFETY_BACKUP="${TARGET_DATA_DIR}.pre_restore_${TIMESTAMP}"
        log_info "Creating safety backup of current data: ${SAFETY_BACKUP}"
        mv "${TARGET_DATA_DIR}" "${SAFETY_BACKUP}"
    fi

    # Create target directory
    mkdir -p "${TARGET_DATA_DIR}"

    # Restore data
    log_info "Restoring data..."

    # Restore topics
    if [ -d "${BACKUP_PATH}/topics" ]; then
        log_info "  - Restoring topics..."
        cp -r "${BACKUP_PATH}/topics" "${TARGET_DATA_DIR}/"
    fi

    # Restore consumer groups
    if [ -d "${BACKUP_PATH}/groups" ]; then
        log_info "  - Restoring consumer groups..."
        cp -r "${BACKUP_PATH}/groups" "${TARGET_DATA_DIR}/"
    fi

    # Restore WAL
    if [ -d "${BACKUP_PATH}/wal" ]; then
        log_info "  - Restoring WAL..."
        cp -r "${BACKUP_PATH}/wal" "${TARGET_DATA_DIR}/"
    fi

    # Restore cluster state
    if [ -d "${BACKUP_PATH}/cluster" ]; then
        log_info "  - Restoring cluster state..."
        cp -r "${BACKUP_PATH}/cluster" "${TARGET_DATA_DIR}/"
    fi

    # Restore metadata files
    for f in "${BACKUP_PATH}"/*.json "${BACKUP_PATH}"/*.yaml; do
        if [ -f "$f" ] && [ "$(basename "$f")" != "manifest.json" ] && [ "$(basename "$f")" != "checksums.sha256" ]; then
            log_info "  - Restoring $(basename "$f")..."
            cp "$f" "${TARGET_DATA_DIR}/"
        fi
    done

    # Restore configuration (optional)
    if [ -d "${BACKUP_PATH}/config" ]; then
        log_info ""
        read -p "Restore configuration files to ${CONFIG_DIR}? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            mkdir -p "${CONFIG_DIR}"
            cp -r "${BACKUP_PATH}/config/"* "${CONFIG_DIR}/"
            log_info "  Configuration restored."
        fi
    fi

    # Set ownership if running as root
    if [ "$(id -u)" -eq 0 ]; then
        if id "streamline" > /dev/null 2>&1; then
            log_info "Setting ownership to streamline user..."
            chown -R streamline:streamline "${TARGET_DATA_DIR}"
        fi
    fi

    log_info ""
    log_info "Restore complete!"
    log_info "  Data restored to: ${TARGET_DATA_DIR}"
    log_info ""
    log_info "You can now start Streamline:"
    log_info "  sudo systemctl start streamline"
}

list_backups() {
    log_info "Available backups in ${BACKUP_DIR}:"
    echo ""

    if [ ! -d "${BACKUP_DIR}" ]; then
        log_warn "Backup directory does not exist: ${BACKUP_DIR}"
        return
    fi

    local count=0
    for archive in "${BACKUP_DIR}"/*.tar.gz; do
        if [ -f "$archive" ]; then
            local name=$(basename "$archive")
            local size=$(du -h "$archive" | cut -f1)
            local date=$(stat -c %y "$archive" 2>/dev/null || stat -f "%Sm" "$archive" 2>/dev/null || echo "unknown")
            printf "  %-50s %8s  %s\n" "$name" "$size" "$date"
            count=$((count + 1))
        fi
    done

    if [ $count -eq 0 ]; then
        log_warn "No backups found."
    else
        echo ""
        log_info "Total: $count backup(s)"
    fi
}

verify_backup() {
    local ARCHIVE="${1}"

    if [ -z "${ARCHIVE}" ]; then
        log_error "Usage: $0 verify <backup_archive>"
        exit 1
    fi

    if [ ! -f "${ARCHIVE}" ]; then
        if [ -f "${BACKUP_DIR}/${ARCHIVE}" ]; then
            ARCHIVE="${BACKUP_DIR}/${ARCHIVE}"
        elif [ -f "${BACKUP_DIR}/${ARCHIVE}.tar.gz" ]; then
            ARCHIVE="${BACKUP_DIR}/${ARCHIVE}.tar.gz"
        else
            log_error "Backup archive not found: ${ARCHIVE}"
            exit 1
        fi
    fi

    log_info "Verifying backup: ${ARCHIVE}"

    # Create temporary extraction directory
    local TEMP_DIR=$(mktemp -d)
    trap "rm -rf ${TEMP_DIR}" EXIT

    # Extract archive
    log_info "Extracting archive..."
    if ! tar -xzf "${ARCHIVE}" -C "${TEMP_DIR}" 2>/dev/null; then
        log_error "Failed to extract archive. File may be corrupted."
        exit 1
    fi
    log_info "  Archive extracted successfully"

    # Find extracted directory
    local BACKUP_NAME=$(ls "${TEMP_DIR}")
    local BACKUP_PATH="${TEMP_DIR}/${BACKUP_NAME}"

    # Check manifest
    if [ -f "${BACKUP_PATH}/manifest.json" ]; then
        log_info "  Manifest found"
        log_info "    Backup name: $(grep -o '"backup_name"[^,]*' "${BACKUP_PATH}/manifest.json" | cut -d'"' -f4)"
        log_info "    Timestamp: $(grep -o '"timestamp"[^,]*' "${BACKUP_PATH}/manifest.json" | cut -d'"' -f4)"
    else
        log_warn "  No manifest found"
    fi

    # Verify checksums
    if [ -f "${BACKUP_PATH}/checksums.sha256" ]; then
        log_info "Verifying file checksums..."
        cd "${BACKUP_PATH}"
        local failed=0
        while IFS= read -r line; do
            local checksum=$(echo "$line" | cut -d' ' -f1)
            local file=$(echo "$line" | cut -d' ' -f3-)
            if [ -f "$file" ]; then
                local actual=$(sha256sum "$file" | cut -d' ' -f1)
                if [ "$checksum" = "$actual" ]; then
                    log_info "  OK: $file"
                else
                    log_error "  FAIL: $file (checksum mismatch)"
                    failed=$((failed + 1))
                fi
            else
                log_error "  MISSING: $file"
                failed=$((failed + 1))
            fi
        done < checksums.sha256
        cd - > /dev/null

        if [ $failed -eq 0 ]; then
            log_info ""
            log_info "Verification PASSED: All files intact"
        else
            log_error ""
            log_error "Verification FAILED: $failed file(s) corrupted or missing"
            exit 1
        fi
    else
        log_warn "No checksums file found. Cannot verify integrity."
    fi

    # Report contents
    log_info ""
    log_info "Backup contents:"
    [ -d "${BACKUP_PATH}/topics" ] && log_info "  - topics/ ($(find "${BACKUP_PATH}/topics" -type f | wc -l) files)"
    [ -d "${BACKUP_PATH}/groups" ] && log_info "  - groups/ ($(find "${BACKUP_PATH}/groups" -type f | wc -l) files)"
    [ -d "${BACKUP_PATH}/wal" ] && log_info "  - wal/"
    [ -d "${BACKUP_PATH}/cluster" ] && log_info "  - cluster/"
    [ -d "${BACKUP_PATH}/config" ] && log_info "  - config/"
}

cleanup_old_backups() {
    if [ "${RETENTION_DAYS}" -gt 0 ]; then
        log_info "Cleaning up backups older than ${RETENTION_DAYS} days..."
        find "${BACKUP_DIR}" -name "*.tar.gz" -mtime +${RETENTION_DAYS} -delete 2>/dev/null || true
    fi
}

usage() {
    echo "Streamline Backup and Restore Script"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  backup [name]           Create a backup (optional custom name)"
    echo "  restore <archive> [dir] Restore from a backup archive"
    echo "  list                    List available backups"
    echo "  verify <archive>        Verify backup integrity"
    echo ""
    echo "Environment Variables:"
    echo "  DATA_DIR        Data directory (default: /var/lib/streamline)"
    echo "  BACKUP_DIR      Backup directory (default: /var/backups/streamline)"
    echo "  CONFIG_DIR      Config directory (default: /etc/streamline)"
    echo "  RETENTION_DAYS  Days to keep backups (default: 7, 0=forever)"
    echo ""
    echo "Examples:"
    echo "  $0 backup                           # Create timestamped backup"
    echo "  $0 backup my_backup                 # Create named backup"
    echo "  $0 restore streamline_backup_20240101_120000.tar.gz"
    echo "  $0 list                             # List all backups"
    echo "  $0 verify my_backup.tar.gz          # Verify backup integrity"
    echo ""
    echo "Notes:"
    echo "  - Stop Streamline before backup/restore for consistency"
    echo "  - Backups are compressed with gzip"
    echo "  - SHA256 checksums verify backup integrity"
}

case "${1:-}" in
    backup)
        backup "$2"
        ;;
    restore)
        restore "$2" "$3"
        ;;
    list)
        list_backups
        ;;
    verify)
        verify_backup "$2"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        usage
        exit 1
        ;;
esac
