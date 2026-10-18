# Changelog

This file records notable user-facing changes to NVIDIA Recovery.

## Unreleased

### Changed

- Accept external code and documentation contributions with maintainer approval
  of non-maintainer CI heads through NVIDIA copy-pr-bot.
- Make GitHub the sole development and CI authority; retire GitLab configuration.
- Separate public PR checks from optional private Attribution qualification.
- Read the Attribution adapter's CA, client certificate, and key from the
  standard `ca.crt`, `tls.crt`, and `tls.key` Secret keys instead of
  `.ca_cert`, `.cert_file`, and `.key_file`. Set the new `*_secret_key_name`
  values to keep reading other key names.


### Added

- NVIDIA open source community health, closed-contribution, governance,
  support, and repository collaboration policies.

NVIDIA Recovery has not yet published a stable OSS release.
