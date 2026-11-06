import logging
import os

from langchain_aws import ChatBedrockConverse

from helicalbi.integration.BaseInterface import LLMFactory

logger = logging.getLogger(__name__)


class BedrockFactory(LLMFactory):
    """Claude on Amazon Bedrock through the Converse API.

    Credentials come from the standard AWS chain (AWS_PROFILE plus a mounted
    ~/.aws holding only the aml-bq2 profile); no key is stored in llm_config.yaml.
    """

    def __init__(self, config):
        self.config = config

    def create_llm(self):
        region = (
            self.config.get("region")
            or os.getenv("BEDROCK_REGION")
            or os.getenv("AWS_REGION")
            or "us-east-1"
        )
        return ChatBedrockConverse(
            model=self.config["model"],
            region_name=region,
            credentials_profile_name=self.config.get("profile") or os.getenv("AWS_PROFILE"),
            temperature=self.config.get("temperature"),
            max_tokens=self.config.get("max_tokens"),
        )
