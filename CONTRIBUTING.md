# Contributing

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | per `go.mod` |
| golangci-lint | latest |
| make | any |

## Getting Started

```sh
git clone https://github.com/urmzd/opentag
cd opentag
make init
```

## Development

```sh
make check   # fmt + lint + test (quality gate)
make test    # go test ./...
make fmt     # gofmt -w .
make run     # build and run the server
```

The sandbox (`pkg/sandbox`) is a security boundary: changes touching it must include tests that prove confinement still holds.

## Commit Convention

Conventional commits (Angular style): `feat:`, `fix:`, `docs:`, `chore:`, etc. Releases are cut automatically from commit types by [sr](https://github.com/urmzd/sr).

## Pull Requests

Fork, branch from `main`, make your change, ensure `make check` passes, and open a PR. CI must be green before review.

## Code Style

Idiomatic Go. Small interfaces, one concern per package, errors wrapped with context and sentinel errors for callers to match on. See AGENTS.md for the full rules.
