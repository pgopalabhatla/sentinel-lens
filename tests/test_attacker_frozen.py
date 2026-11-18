"""The attacker agent is frozen (tag `attacker-freeze-v2`; v1 is history): the committed docs/attacker-config-frozen.md must be
exactly what scripts/attacker_freeze.py generates from the code now. If this fails, the attacker changed:
either undo the change, or regenerate the file under a new tag with a log entry saying why."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def test_the_frozen_attacker_config_matches_the_code():
    import attacker_freeze
    assert attacker_freeze.OUT.exists(), "docs/attacker-config-frozen.md is missing"
    assert attacker_freeze.OUT.read_text() == attacker_freeze.generate(), \
        "the attacker no longer matches docs/attacker-config-frozen.md (see the rule in that file)"


def test_the_frozen_file_holds_the_prompt_the_budget_and_every_tool():
    import attacker_freeze
    from sentinel_stream import attacker as A
    text = attacker_freeze.OUT.read_text()
    prompt = A.system_prompt(A.Config.load())
    assert prompt in text and attacker_freeze.sha256(prompt) in text and "{gap}" not in prompt
    assert all(f"`{d['name']}`" in text for d in A.definitions())
    assert "| minimum gap between injection starts | 180 min | in code |" in text and "attacker-freeze-v2" in text
    assert "| named scenarios, share of the campaign's injections at every point, at most | 60% | in code |" in text
    assert "{named_share}" not in prompt and "at most 60% of the campaign's injections" in prompt
