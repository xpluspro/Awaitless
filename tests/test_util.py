import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from awaitless.backends.ssh import SSHBackend
from awaitless.config import Settings, ssh_target_and_options
from awaitless.util import parse_duration


class DurationTest(unittest.TestCase):
    def test_units(self) -> None:
        self.assertEqual(parse_duration("30s"), 30)
        self.assertEqual(parse_duration("2m"), 120)
        self.assertEqual(parse_duration("1.5h"), 5400)

    def test_invalid(self) -> None:
        with self.assertRaises(ValueError):
            parse_duration("soon")


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
