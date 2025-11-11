### How to generate a private key and a self-signed certificate

ECDSA with the P-256 curve – is supported by web browsers

```shell
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:prime256v1 -out logrep.key
```

Alternative: ED25519 – no support in web browsers yet, but works with logrep_client

```shell
openssl genpkey -algorithm ED25519 -out logrep.key
```

Self-signed certificate, based on the private key

```shell
openssl req -new -x509 -sha256 -key logrep.key -out logrep.crt -days 365 -subj "/CN=$(hostname)" -addext "subjectAltName=DNS:$(hostname),DNS:localhost,IP:127.0.0.1"
```

Add `,IP:$(hostname -i)` to subjectAltName if you want to access your server via IP address; requires `hostname` from GNU inetutils

```shell
openssl req -new -x509 -sha256 -key logrep.key -out logrep.crt -days 365 -subj "/CN=$(hostname)" -addext "subjectAltName=DNS:$(hostname),DNS:localhost,IP:127.0.0.1,IP:$(hostname -i)"
```

---
Copyright © 2025 [macmarrum](https://github.com/macmarrum)\
SPDX-License-Identifier: [GPL-3.0-or-later](/LICENSES)
