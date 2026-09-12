"""Linux proc parser regressions; synthetic bytes do not certify isolation."""

import pytest

from dpone.adapters.composition_clickhouse_supervisor_linux import parse_listeners, parse_mountinfo, parse_process_stat
from dpone.contracts.composition_identity import CompositionAdmissionError


def test_process_stat_uses_final_parenthesis_and_exact_starttime():
    fields = ["S", "1", "42", "42"] + ["0"] * 15 + ["9876"]
    assert parse_process_stat(("42 (name with ) spaces) " + " ".join(fields)).encode(), 42)["start_ticks"] == 9876


def test_truncated_process_stat_never_defaults_to_new_identity():
    with pytest.raises(CompositionAdmissionError):
        parse_process_stat(b"42 (x) S 1 2", 42)


def test_tcp_listener_decodes_loopback_and_inode():
    raw = b"  sl  local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n   0: 0100007F:1FBD 00000000:0000 0A 00000000:00000000 00:00000000 00000000 101 0 9001\n"
    assert parse_listeners(raw, ipv6=False) == (("127.0.0.1", 8125, 9001, 101),)


def test_mountinfo_retains_readonly_and_decodes_escaped_names():
    value = parse_mountinfo(b"12 1 8:1 /file /etc/my\\040config ro,nosuid - ext4 /dev/sda1 rw\n")
    assert value[0]["destination"] == "/etc/my config" and value[0]["options"] == ("nosuid", "ro")


def test_deadline_and_non_linux_are_rejected_before_proc_read(monkeypatch):
    import time

    import dpone.adapters.composition_clickhouse_supervisor_linux as module

    probe = module.LinuxSupervisorProbe()
    monkeypatch.setattr(module.sys, "platform", "unsupported")
    with pytest.raises(CompositionAdmissionError, match="linux_deadline"):
        probe.host_boot(time.monotonic() + 1)
    monkeypatch.setattr(module.sys, "platform", "linux")
    with pytest.raises(CompositionAdmissionError, match="linux_deadline"):
        probe.host_boot(time.monotonic() - 1)


def test_config_tree_hashes_original_bytes_and_rejects_symlinks(tmp_path, monkeypatch):
    import time

    import dpone.adapters.composition_clickhouse_supervisor_linux as module

    monkeypatch.setattr(module.sys, "platform", "linux")
    probe = module.LinuxSupervisorProbe()
    probe._proc = tmp_path
    root = tmp_path / "10" / "root" / "config"
    root.mkdir(parents=True)
    (root / "server.xml").write_bytes(b"<clickhouse/>")
    before = probe.config_tree(10, "/config", time.monotonic() + 2)
    assert before[1]["sha256"] == module.digest(b"<clickhouse/>")
    (root / "server.xml").write_bytes(b"<clickhouse>changed</clickhouse>")
    assert probe.config_tree(10, "/config", time.monotonic() + 2) != before
    (root / "alias").symlink_to(root / "server.xml")
    with pytest.raises(CompositionAdmissionError, match="config_symlink"):
        probe.config_tree(10, "/config", time.monotonic() + 2)


def test_duplicate_mount_and_malformed_socket_rows_never_count_as_absence():
    line = b"12 1 8:1 /file /config ro - ext4 /dev/sda1 rw\n"
    with pytest.raises(CompositionAdmissionError, match="mount_inventory"):
        parse_mountinfo(line + line)
    with pytest.raises(CompositionAdmissionError):
        parse_listeners(b"local_address\ninvalid\n", ipv6=False)


def test_process_inventory_includes_each_thread_namespace(tmp_path, monkeypatch):
    import time

    import dpone.adapters.composition_clickhouse_supervisor_linux as module

    monkeypatch.setattr(module.sys, "platform", "linux")
    probe = module.LinuxSupervisorProbe()
    probe._proc = tmp_path
    (tmp_path / "10" / "task" / "10").mkdir(parents=True)
    (tmp_path / "10" / "task" / "11").mkdir()
    assert probe.processes(time.monotonic() + 1) == (10, 11)
