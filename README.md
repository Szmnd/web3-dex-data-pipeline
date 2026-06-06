# Asynchronous Web3 DEX Data Pipeline

An enterprise-grade, asynchronous data pipeline built in Python to fetch, process, and analyze real-time market structures and volume anomalies across decentralized liquidity pools.

## Technical Architecture & Features
- **Asynchronous Concurrency**: Built using `asyncio` and `aiohttp` to perform non-blocking I/O operations, drastically reducing network bottlenecks during multi-pool data extraction.
- **Statistical Analytics**: Leverages `pandas` for advanced data alignment (merging timeline price/volume feeds) and structuring.
- **Anomaly Detection**: Utilizes `numpy` vectorization to calculate rolling Z-scores on transactional volumes, automatically flagging execution anomalies (e.g., flash-loan spikes or sudden liquidity drains).
- **Robust Exception Handling**: Implements defensive programming blocks with complete logging metrics to ensure the pipeline's resilience during API rate-limiting or peer drops.

## Technology Stack
- **Language**: Python 3.10+
- **Core Libraries**: `pandas`, `numpy`, `aiohttp`, `asyncio`

## Quick Start
1. Clone the repository.
2. Install dependencies: `pip install pandas numpy aiohttp`
3. Execute the script: `python pipeline.py`
