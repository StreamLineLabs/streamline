# Anomaly Alerting Example

Real-time anomaly detection on a metrics stream.

## Run
```python
from streamline_sdk.ai import AIClient

ai = AIClient("http://localhost:9094")

async for alert in ai.detect_anomalies("server-metrics", threshold=2.5):
    print(f"🚨 Anomaly: {alert.field}={alert.value} (z-score: {alert.z_score:.2f})")
    # Send to Slack, PagerDuty, etc.
```
