# sidesui-consensus

Lightweight BFT consensus for Side-SUI private consortium networks.

## Overview

This crate provides a simplified consensus layer for Side-SUI, designed to replace
Narwhal/Bullshark for private consortium deployments where:

- Validators are trusted (3-11 nodes)
- Transaction volume is moderate (<10k TPS)
- Resource efficiency is prioritized over maximum throughput

## Target Resources

| Metric | Full SUI | Side-SUI |
|--------|----------|----------|
| RAM | 128GB | 16-32GB |
| CPU | 36 cores | 4-8 cores |
| Storage growth | ~7GB/day | <1GB/day |

## Architecture

```
+-------------------+
|  SideSuiConsensus |
+-------------------+
         |
         v
+-------------------+
| Transaction Queue |
+-------------------+
         |
         v
+-------------------+     +-------------------+
|   Block Producer  |<--->|  Voting Protocol  |
+-------------------+     +-------------------+
         |                         |
         v                         v
+-------------------+     +-------------------+
|   Persistence     |     |     Network       |
|   (WAL)           |     |   (gRPC/TCP)      |
+-------------------+     +-------------------+
```

## Development Phases

- [x] Phase 1: Analysis & interface definition
- [ ] Phase 2a: Network transport (tonic/gRPC)
- [ ] Phase 2b: Voting protocol (simple BFT)
- [ ] Phase 2c: Persistence (RocksDB WAL)
- [ ] Phase 2d: Epoch management integration
- [ ] Phase 3: Testing & benchmarking

## Usage

```rust
use sidesui_consensus::{SideSuiConsensus, SideSuiConfig};

let config = SideSuiConfig {
    authority: my_authority,
    validators: vec![...],
    listen_addr: "0.0.0.0:8080".to_string(),
    block_interval_ms: 1000,
    max_txns_per_block: 1000,
};

let consensus = SideSuiConsensus::new(config);

// Submit transactions
consensus.submit_to_consensus(&transactions)?;
```

## License

Apache-2.0
