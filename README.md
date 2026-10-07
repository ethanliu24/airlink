# airlink
Share files across machines locally

## Dependencies
- [Go 1.26.5](https://go.dev/doc/install)
- [Protobuf](https://protobuf.dev/downloads/)
    - Mac homebrew: `brew install protobuf`
    - Linux: `sudo apt-get install -y protobuf-compiler`
    - Windows: [Precompiled binary](https://github.com/protocolbuffers/protobuf/releases)

## Compile proto files

Stay at project root and run: `protoc --go_out=. --go_opt=paths=source_relative <path-to-.proto>`
