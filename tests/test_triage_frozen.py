"""The triage agent is frozen (tag `triage-freeze-v1`): the committed docs/triage-config-frozen.md must be
exactly what scripts/triage_freeze.py generates from the code now. If this fails, the agent changed:
either undo the change, or regenerate the file under a new tag with a log entry saying why."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


def test_the_frozen_triage_config_matches_the_code():
    import triage_freeze
    assert triage_freeze.OUT.exists(), "docs/triage-config-frozen.md is missing"
    assert triage_freeze.OUT.read_text() == triage_freeze.generate(), \
        "the triage agent no longer matches docs/triage-config-frozen.md (see the rule in that file)"


def test_the_frozen_file_holds_the_prompt_the_guard_and_every_tool():
    import triage_freeze
    from sentinel_stream import context_tools, investigator
    text = triage_freeze.OUT.read_text()
    assert investigator.SYSTEM in text and triage_freeze.sha256(investigator.SYSTEM) in text
    assert "def guard(" in text and all(f"`{t.name}`" in text for t in context_tools.TOOLS)
    assert "follow_up_minutes | 10" in text and "triage-freeze-v1" in text
