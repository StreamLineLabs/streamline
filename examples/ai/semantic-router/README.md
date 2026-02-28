# Semantic Router Example

Route messages to different topics based on their semantic meaning.

## Run
```python
from streamline_sdk.ai import AIClient

ai = AIClient("http://localhost:9094")

# Route incoming messages by meaning
async for msg in client.consume("incoming"):
    results = await ai.search(msg.value, topic="routing-rules", top_k=1)
    target_topic = results[0].value["target_topic"]
    await client.produce(target_topic, msg.value)
```
