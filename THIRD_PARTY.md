# Third-party software

Direct runtime dependencies are resolved through Go modules and remain governed by their own
licenses. They are not vendored into this repository.

| Component | Version | License |
| --- | --- | --- |
| `connectrpc.com/connect` | 1.18.1 | Apache-2.0 |
| `github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ags` | 1.3.173 | Apache-2.0 |
| `github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common` | 1.3.175 | Apache-2.0 |
| `github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/monitor` | 1.3.171 | Apache-2.0 |
| `google.golang.org/protobuf` | 1.36.7 | BSD-3-Clause |

The repository includes modified protocol definitions derived from E2B Infrastructure and Go
code generated from those definitions. See [SOURCE_ATTRIBUTION.md](SOURCE_ATTRIBUTION.md) for the
source revision, modifications, hashes, and generation relationship.

Verification installs pinned, non-runtime tools through `make tools`: Buf 1.47.2 (Apache-2.0),
`protoc-gen-go` 1.36.11 (BSD-3-Clause), `protoc-gen-connect-go` 1.18.1 (Apache-2.0), and Gitleaks
8.30.0 (MIT). These binaries are not vendored or shipped with the SDK.

The root [LICENSE](LICENSE) governs this project's original work. This inventory does not replace
the license text or alter third-party terms.
