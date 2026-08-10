import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from awaitless.backends.ssh import SSHBackend
from awaitless.config import Settings, ssh_target_and_options
from awaitless.util import parse_duration, parse_time


class DurationTest(unittest.TestCase):
    def test_units(self) -> None:
        self.assertEqual(parse_duration("30s"), 30)
        self.assertEqual(parse_duration("2m"), 120)
        self.assertEqual(parse_duration("1.5h"), 5400)

    def test_invalid(self) -> None:
        with self.assertRaises(ValueError):
            parse_duration("soon")


class TimeTest(unittest.TestCase):
    def test_gnu_date_nanoseconds_are_truncated_to_microseconds(self) -> None:
        self.assertEqual(
            parse_time("2026-08-10T02:47:35.156488983Z"),
            datetime(2026, 8, 10, 2, 47, 35, 156488, tzinfo=timezone.utc),
        )

    def test_timezone_offset_is_preserved_when_truncating(self) -> None:
        parsed = parse_time("2026-08-10T10:47:35.123456789+08:00")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.microsecond, 123456)
        self.assertEqual(parsed.utcoffset().total_seconds(), 8 * 60 * 60)


class SSHConfigTest(unittest.TestCase):
    def test_host_authentication_and_timeout_options(self) -> None:
        settings = Settings(
            data_dir=Path("/tmp/awaitless-test"),
            hosts={
                "cluster": {
                    "hostname": "login.example",
                    "gssapi_authentication": False,
                    "connect_timeout": 8,
                }
            },
        )
        target, options, _ = ssh_target_and_options(settings, "cluster")
        self.assertEqual(target, "login.example")
        self.assertIn("GSSAPIAuthentication=no", options)
        self.assertIn("ConnectTimeout=8", options)

    def test_operation_timeout_can_cover_slow_login_nodes(self) -> None:
        settings = Settings(
            data_dir=Path("/tmp/awaitless-test"),
            hosts={"cluster": {"operation_timeout": 20}},
        )
        backend = SSHBackend(Mock(), settings)
        completed = Mock(returncode=0, stdout="ok", stderr="")
        with patch("awaitless.backends.ssh.subprocess.run", return_value=completed) as run:
            self.assertEqual(backend._invoke("cluster", ":", timeout=5), "ok")
        self.assertEqual(run.call_args.kwargs["timeout"], 20)
