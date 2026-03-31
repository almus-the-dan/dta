cargo fmt --check `
    && cargo clippy --all-features -- -Dwarnings `
    && cargo test --all-features