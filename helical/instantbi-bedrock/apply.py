"""Add a Bedrock provider to Helical Insight's Instant BI (the AI chat).

Run from anywhere:  python helical/instantbi-bedrock/apply.py [HELICAL_DIR]
HELICAL_DIR defaults to ~/helical/helicalinsight-7.0.0-docker.

Idempotent. Backs up each file it edits to <file>.pre-bedrock once.
Then: docker compose up -d --force-recreate instantbi  (from HELICAL_DIR)
"""
import re
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MODEL = "global.anthropic.claude-haiku-4-5-20251001-v1:0"

helical = Path(sys.argv[1] if len(sys.argv) > 1 else "~/helical/helicalinsight-7.0.0-docker").expanduser()
app = helical / "instantbi" / "helicalbi"
pkg = app / "helicalbi"


def backup(p: Path) -> None:
    b = p.with_name(p.name + ".pre-bedrock")
    if not b.exists():
        shutil.copy2(p, b)


def edit(p: Path, fn) -> None:
    old = p.read_text()
    new = fn(old)
    if new != old:
        backup(p)
        p.write_text(new)
        print("edited", p.relative_to(helical))
    else:
        print("ok    ", p.relative_to(helical))


# 1. factory package
dst = pkg / "integration" / "bedrock"
dst.mkdir(exist_ok=True)
for f in (HERE / "integration" / "bedrock").glob("*.py"):
    shutil.copy2(f, dst / f.name)
print("copied integration/bedrock")

# 2. LLM resolver
def llm_resolver(s: str) -> str:
    if "BedrockFactory" in s:
        return s
    s = s.replace(
        "from helicalbi.integration.anthropic.AnthropicFactory import AnthropicFactory\n",
        "from helicalbi.integration.anthropic.AnthropicFactory import AnthropicFactory\n"
        "from helicalbi.integration.bedrock.BedrockFactory import BedrockFactory\n",
    )
    return s.replace(
        '        elif provider == "ollama":',
        '        elif provider == "bedrock":\n            return BedrockFactory(config)\n        elif provider == "ollama":',
    )

edit(pkg / "core" / "LLMFactoryResolver.py", llm_resolver)

# 3. token-usage resolver
def tok_resolver(s: str) -> str:
    if "BedrockTokenUsageFactory" in s:
        return s
    s = s.replace(
        "from helicalbi.integration.anthropic.AnthropicTokenUsageFactory import AnthropicTokenUsageFactory\n",
        "from helicalbi.integration.anthropic.AnthropicTokenUsageFactory import AnthropicTokenUsageFactory\n"
        "from helicalbi.integration.bedrock.BedrockTokenUsageFactory import BedrockTokenUsageFactory\n",
    )
    return s.replace(
        '        "anthropic": AnthropicTokenUsageFactory,\n',
        '        "anthropic": AnthropicTokenUsageFactory,\n        "bedrock": BedrockTokenUsageFactory,\n',
    )

edit(pkg / "core" / "TokenUsageFactoryResolver.py", tok_resolver)

# 4. factory registry
edit(
    pkg / "config" / "llm_factory_registry.yaml",
    lambda s: s if "bedrock:" in s else s.rstrip("\n")
    + "\n  bedrock: helicalbi.integration.bedrock.BedrockFactory.BedrockFactory\n",
)

# 5. llm_config: default provider + bedrock block (no key; AWS profile chain)
def llm_config(s: str) -> str:
    s = re.sub(r"(?m)^default_provider:.*$", "default_provider: bedrock", s)
    if re.search(r"(?m)^  bedrock:", s):
        return s
    block = (
        "  bedrock:\n"
        f"    model: {MODEL}\n"
        "    region: us-east-1\n"
        "    temperature: 0.1\n"
        "    max_tokens: 4000\n\n"
    )
    return s.replace("providers:\n", "providers:\n" + block, 1)

edit(pkg / "config" / "llm_config.yaml", llm_config)

# 6. requirements
edit(
    app / "requirements.txt",
    lambda s: s if "langchain-aws" in s else s.rstrip("\n") + "\nlangchain-aws\nboto3\n",
)

# 6b. Ollama import: current langchain-community no longer exports ChatOllama,
# which breaks the whole app at import time (requirements are unpinned). Import lazily.
def ollama_lazy(s: str) -> str:
    if "# lazy import" in s:
        return s
    s = s.replace("from langchain_community.chat_models import ChatOllama\n\n", "")
    return s.replace(
        "        return ChatOllama(",
        "        try:  # lazy import: only needed when the ollama provider is used\n"
        "            from langchain_ollama import ChatOllama\n"
        "        except ImportError:\n"
        "            from langchain_community.chat_models import ChatOllama  # lazy import\n"
        "        return ChatOllama(",
    )

edit(pkg / "integration" / "ollama" / "OllamaFactory.py", ollama_lazy)

# 7. compose override: merge the instantbi service in
ov = helical / "docker-compose.override.yml"
add = (HERE / "docker-compose.instantbi.yml").read_text()
def override(s: str) -> str:
    if "instantbi:" in s:
        return s
    svc = add.split("services:\n", 1)[1]
    return s.rstrip("\n") + "\n" + svc

edit(ov, override)
print("\nNext: cd", helical, "&& docker compose up -d --force-recreate instantbi")
