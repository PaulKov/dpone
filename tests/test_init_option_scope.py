"""Shared init argument actions preserve target-scoped compatibility."""

import argparse

import pytest


def test_tracker_keeps_recipe_profile_scope():
    from dpone.commands.init_option_scope import TRACKED_OPTIONS_ATTR, _TrackedInitOption

    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", action=_TrackedInitOption)
    args = parser.parse_args(["--profile", "recipe@1.0.0"])
    assert args.profile == "recipe@1.0.0"
    assert getattr(args, TRACKED_OPTIONS_ATTR) == (("--profile", frozenset({"pipeline"})),)


def test_rejection_retains_target_diagnostic(capsys):
    from dpone.commands.init_option_scope import _RejectInitOption

    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", action=_RejectInitOption, init_target="project")
    with pytest.raises(SystemExit) as error:
        parser.parse_args(["--profile", "recipe@1.0.0"])
    assert error.value.code == 2
    assert "project does not accept: --profile" in capsys.readouterr().err
