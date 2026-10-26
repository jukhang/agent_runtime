"""Disposable certificates for real TLS acceptance; never production credentials."""

import hashlib
import ssl
import subprocess
from pathlib import Path


def certificates(directory: Path) -> dict[str, str]:
    directory.mkdir(parents=True, exist_ok=True)

    def run(*args):
        subprocess.run(["openssl", *args], cwd=directory, check=True, capture_output=True)

    run(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "ca.key",
        "-out",
        "ca.pem",
        "-days",
        "1",
        "-subj",
        "/CN=Disposable Broker CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
    )
    fingerprints = {}
    for name, purpose in (
        ("broker", "serverAuth"),
        ("relay", "clientAuth"),
        ("other", "clientAuth"),
        ("egress", "serverAuth"),
    ):
        run(
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
            "-subj",
            f"/CN={name}.test",
            "-addext",
            f"subjectAltName=DNS:{name}.test",
            "-addext",
            f"extendedKeyUsage={purpose}",
        )
        run(
            "x509",
            "-req",
            "-in",
            f"{name}.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            f"{name}.pem",
            "-days",
            "1",
            "-copy_extensions",
            "copy",
        )
        der = ssl.PEM_cert_to_DER_cert((directory / f"{name}.pem").read_text())
        fingerprints[name] = hashlib.sha256(der).hexdigest()
    for key in directory.glob("*.key"):
        key.chmod(0o600)
    return fingerprints
