from __future__ import annotations

from typing import Any, Optional

from helicalbi.integration.TokenUsageFactory import BaseTokenUsageFactory
from helicalbi.model.TokenUsage import TokenUsage


class BedrockTokenUsageFactory(BaseTokenUsageFactory):
    """Token usage for ``ChatBedrockConverse`` responses (usage_metadata carries the counts)."""

    def from_ai_message(self, message: Any) -> TokenUsage:
        # usage_metadata has the counts; response_metadata (model_name) fills in the model.
        usage_metadata = getattr(message, "usage_metadata", None)
        response_metadata = getattr(message, "response_metadata", None) or {}
        if usage_metadata:
            return self._build({**response_metadata, **usage_metadata})
        return self.from_response_metadata(response_metadata)

    def from_response_metadata(self, metadata: Optional[dict[str, Any]]) -> TokenUsage:
        if not metadata:
            return TokenUsage()
        usage = metadata.get("usage") or {}
        merged = {
            **metadata,
            "input_tokens": usage.get("inputTokens"),
            "output_tokens": usage.get("outputTokens"),
            "total_tokens": usage.get("totalTokens"),
            "model_name": metadata.get("model_id") or metadata.get("model_name"),
        }
        return self._build(merged)
