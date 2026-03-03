import os
import pytest


class TestConfig:
    """Tests for config.py — environment variable loading and defaults."""

    def test_default_llm_base_url(self, monkeypatch):
        """LLM_BASE_URL defaults to https://api.deepseek.com/v1 when not set."""
        monkeypatch.delenv("LLM_BASE_URL", raising=False)
        monkeypatch.delenv("LLM_API_KEY", raising=False)
        monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
        monkeypatch.delenv("LLM_MODEL", raising=False)
        monkeypatch.delenv("PORT", raising=False)
        # Re-import to pick up env changes
        import importlib
        import server_py.config as cfg
        importlib.reload(cfg)
        assert cfg.LLM_BASE_URL == "https://api.deepseek.com/v1"

    def test_default_port(self, monkeypatch):
        """PORT defaults to 3001 when not set."""
        monkeypatch.delenv("PORT", raising=False)
        import importlib
        import server_py.config as cfg
        importlib.reload(cfg)
        assert cfg.PORT == 3001

    def test_default_llm_model(self, monkeypatch):
        """LLM_MODEL defaults to deepseek-chat when not set."""
        monkeypatch.delenv("LLM_MODEL", raising=False)
        import importlib
        import server_py.config as cfg
        importlib.reload(cfg)
        assert cfg.LLM_MODEL == "deepseek-chat"

    def test_llm_api_key_falls_back_to_deepseek_api_key(self, monkeypatch):
        """LLM_API_KEY falls back to DEEPSEEK_API_KEY for backward compatibility."""
        monkeypatch.delenv("LLM_API_KEY", raising=False)
        monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-legacy-key")
        import importlib
        import server_py.config as cfg
        importlib.reload(cfg)
        assert cfg.LLM_API_KEY == "sk-legacy-key"

    def test_llm_api_key_takes_precedence(self, monkeypatch):
        """LLM_API_KEY takes precedence over DEEPSEEK_API_KEY."""
        monkeypatch.setenv("LLM_API_KEY", "sk-new-key")
        monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-legacy-key")
        import importlib
        import server_py.config as cfg
        importlib.reload(cfg)
        assert cfg.LLM_API_KEY == "sk-new-key"

    def test_custom_port(self, monkeypatch):
        """PORT can be overridden via environment variable."""
        monkeypatch.setenv("PORT", "8080")
        import importlib
        import server_py.config as cfg
        importlib.reload(cfg)
        assert cfg.PORT == 8080

    def test_custom_llm_base_url(self, monkeypatch):
        """LLM_BASE_URL can be overridden via environment variable."""
        monkeypatch.setenv("LLM_BASE_URL", "http://localhost:11434/v1")
        import importlib
        import server_py.config as cfg
        importlib.reload(cfg)
        assert cfg.LLM_BASE_URL == "http://localhost:11434/v1"

    def test_llm_api_key_defaults_to_empty(self, monkeypatch):
        """LLM_API_KEY defaults to empty string when neither key is set."""
        monkeypatch.delenv("LLM_API_KEY", raising=False)
        monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
        import importlib
        import server_py.config as cfg
        importlib.reload(cfg)
        assert cfg.LLM_API_KEY == ""
