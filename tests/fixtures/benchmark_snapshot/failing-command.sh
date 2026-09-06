#!/bin/sh
printf '%s\n' "${STUB_NAME:-failing-command}" >> "$STUB_LOG"
exit "${STUB_EXIT_CODE:-23}"
