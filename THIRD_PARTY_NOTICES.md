# Third-Party Notices

This repository contains or records the following third-party material. The
NVIDIA Recovery Apache License 2.0 does not replace the license terms that
apply to third-party material.

## Alembic migration templates

The following files adapt Alembic's generic migration template and retain
Michael Bayer's copyright and the full MIT License terms in their source:

- `notifications/notifications/_migrations/script.py.mako`
- `recovery_libs/recovery_libs/storage/alembic/script.py.mako`

The reference checked for this notice is Alembic `rel_1_18_4`, commit
`766b5ee98e0aeea85a18f272a1ac813712ee0697`:
[generic template](https://github.com/sqlalchemy/alembic/blob/766b5ee98e0aeea85a18f272a1ac813712ee0697/alembic/templates/generic/script.py.mako)
and [MIT License](https://github.com/sqlalchemy/alembic/blob/766b5ee98e0aeea85a18f272a1ac813712ee0697/LICENSE).
Comparison with the generic templates in
[Alembic 1.13.3](https://github.com/sqlalchemy/alembic/blob/rel_1_13_3/alembic/templates/generic/script.py.mako)
and [Alembic 1.14.1](https://github.com/sqlalchemy/alembic/blob/rel_1_14_1/alembic/templates/generic/script.py.mako)
confirms that, apart from the added license comments, the `recovery_libs`
template is an exact copy and the notifications template differs only by
`import sqlmodel`. The narrower `down_revision` annotation and absence of
`upgrade`/`downgrade` docstrings are already present in those upstream
versions; they are not local adaptations. Those releases also use the
[MIT License](https://github.com/sqlalchemy/alembic/blob/rel_1_14_1/LICENSE).
The license comments are emitted into generated migrations so the terms
accompany copies of the template-derived code.

Both templates first appear in this repository's initial private preview
commit, `08852ac96`, with those same bodies and without an upstream version
record. The matching releases establish upstream-derived content, but do not
identify which release was originally copied.

## Generated API definitions

The dependency revisions below are pinned in
[`api-gateway/buf.lock`](api-gateway/buf.lock). The listed Go files are generated
from those third-party protocol definitions, not NVIDIA-owned definitions.

### Google APIs

Source: [googleapis/googleapis](https://github.com/googleapis/googleapis),
distributed through the Buf module
[`buf.build/googleapis/googleapis`](https://buf.build/googleapis/googleapis),
commit `61b203b9a9164be9a834f58c37be6f62`.

- `api-gateway/gen/go/heimdall/v1/google/api/annotations.pb.go`
- `api-gateway/gen/go/heimdall/v1/google/api/field_behavior.pb.go`
- `api-gateway/gen/go/heimdall/v1/google/api/http.pb.go`

These files retain their Google copyright notices and Apache License 2.0
source headers. The full license text is provided in the root
[`LICENSE`](LICENSE). The exact pinned Buf module contains an Apache License
2.0 `LICENSE` and no separate `NOTICE` file.

### grpc-gateway OpenAPI options

Source: [grpc-ecosystem/grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway),
distributed through the Buf module
[`buf.build/grpc-ecosystem/grpc-gateway`](https://buf.build/grpc-ecosystem/grpc-gateway),
commit `4c5ba75caaf84e928b7137ae5c18c26a`.

- `api-gateway/gen/go/heimdall/v1/protoc-gen-openapiv2/options/annotations.pb.go`
- `api-gateway/gen/go/heimdall/v1/protoc-gen-openapiv2/options/openapiv2.pb.go`

The exact pinned Buf module is distributed under the BSD 3-Clause License and
contains no separate `NOTICE` file. Its full license text follows:

```text
Copyright (c) 2015, Gengo, Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.

    * Neither the name of Gengo, Inc. nor the names of its
      contributors may be used to endorse or promote products derived from this
      software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

## Vendored Agent Capabilities

Source and license provenance is recorded in
[`agent-capabilities/vendor/README.md`](agent-capabilities/vendor/README.md).
The vendored capability material includes:

- `code-simplifier`, distributed under Apache License 2.0
- `feature-dev`, distributed under Apache License 2.0
- Context7 capability material, distributed under the MIT License

The corresponding license texts are retained under
`agent-capabilities/vendor/`.

## Bundled Helm dependencies

The source release retains the chart dependency lock, upstream license/NOTICE
texts and [bundled chart inventory](deploy/helm/nvidia-recovery/THIRD_PARTY_NOTICES.md).
The umbrella archive includes dependencies even when disabled at runtime.
Those licenses apply to chart source, not to separately obtained container images.
The inventory records unresolved MinIO license clearance; retention is not
approval to redistribute or a complete dependency vulnerability/license audit.
