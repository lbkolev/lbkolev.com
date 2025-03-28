+++
title = "Aggregating cross-chain DEX data"
draft = false
date = 2024-08-16
updated = 2024-08-20

[taxonomies]
tags = ["cs", "cs/rust"]
+++

Ethereum has adopted the [rollup-centric](https://ethereum-magicians.org/t/a-rollup-centric-ethereum-roadmap/4698) roadmap, positioning itself as the global settlement layer for [potential layer-2s](https://l2beat.com/scaling/summary) more so than being direct target for "traditional" layer-1 applications. The move allows layer-2s to acquire the economic security of Ethereum as a base layer, the layer-2s on the other hand provide Ethereum with fees for all the block (and blob!) space required to commit each proof. Having such a clear separation of responsibilities provides the means needed to scale the current biggest bottleneck: transaction throughput. One of the downsides of the approach is the increased scatteredness of liquidity and the sheer amount of liquidity pools getting deployed.

One of the problems we're trying to solve at [Polia](https://polia.io) is how to most efficiently provide direct exposure to real-time data from all relevant (liquidity unpoor) decentralised exchanges, without accruing too much unnecessary noise and pressure for the clients that'll be consuming the data. Doing so would allow anyone the ability to acquire and keep up-to-date a "world-view" of the current state of all decentralised liquidity pools across multiple networks.

The system used to provide the service consists of two core components, both developed in [Rust](https://www.rust-lang.org/):

- **node ETL** - pipeline aggregating filtered live data into an append-only stream
- **websocket gateway** - WS server synthesising and exposing the data for client consumption

{% mermaid() %}
graph LR
A[Networks nodes - Ethereum,Zksync,Arbitrum etc.] -->|Raw Data| B(Node ETL instances)
B -->|Filtered Data| C[FIFO Stream]
C -->|Aggregated Data| D(Websocket Gateway)
D -->|Synthesised Data| E[Clients]

    subgraph "ETL Pipeline"
    B
    C
    end

    subgraph "Data Exposure"
    D
    end

    classDef component fill:#f9f,stroke:#333,stroke-width:2px;
    class B,D component;

{% end %}

<div style="text-align: center; font-style: italic; margin-top: 10px;">system overview</div>

# Node ETL

The Node ETL provides the means to listen, aggregate and transform the raw events notifications received from each [supported network](https://docs.polia.io/#networks-endpoints). Each EVM compatible layer-2 offers the same means to acquire live data as the underlying layer-1 - [pubsub JSON-RPC notifications](https://geth.ethereum.org/docs/interacting-with-geth/rpc/pubsub).
As DEFI is mostly still dominated by uniswap-v2 and uniswap-v3, and the hundreds of clones that have been deployed in their honor, the events we're most interested in to get an idea of the prices of roughly ~80% of all deployed liquidity pools are respectively:

- uniswap-v2 - `event Sync(uint112 reserve0, uint112 reserve1)`
- uniswap-v3 - `event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)`

Each running instance of Node ETL has a `Manager` that keeps a list of collectors for each supported network. The Manager is actively tracking the healthiness and liveness of each collector. With the increase of the number of networks, you'd want to also increase the running instances of `Node ETL` - each supporting different networks, for example, one `Node ETL` instance might handle `Ethereum` and `Arbitrum`, while another manages `Zksync`, `Base`, and `Optimism`. This division allows for optimal resource allocation and prevents any single instance from getting throttled.

```rust
#[derive(Debug, Clone)]
pub struct Manager<N>
where
    N: NetworkT,
{
    pub manager_config: ManagerConfig,
    pub collectors: Vec<Collector<N>>,
    queue_rx: mpsc::Sender<Resource<N::Block, N::Log>>,
}
```

Each collector consists of a node instance, database pool and a queue that'll propagate the events of interest to a dedicated channel that'll process them further. As the websocket connections are fundamentally unstable the node client has to be self-healing and reconnect whenever a connection gets broken up or closed.

```rust
#[derive(Debug, Clone)]
pub struct Collector<N>
where
    N: NetworkT,
{
    config: CollectorConfig,
    node: N,

    #[cfg(feature = "postgres")]
    pool: PgPool,

    #[cfg(feature = "redis-stream")]
    queue_rx: mpsc::Sender<Resource<N::Block, N::Log>>,
}
```

Each node client subscribes for the desired events, by applying criteria (also known as `Filter` in the [official docs](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_newfilter)), transforms the result into internally meaningful representation, inserts it as is into the database and sends it to a dedicated channel for further processing.

```rust
async fn stream_logs(
        &self,
        stop_signal: watch::Receiver<bool>,
        criteria: Option<&Criteria>,
    ) -> crate::Result<()> {
        let mut stream = self.node.sub_logs(criteria).await?;

        while let Some(log) = stream.next().await {
            if stop_signal.has_changed()? {
                break;
            }

            let log = match log {
                Ok(log) => serde_json::from_str::<<N as NetworkT>::Log>(log.get())?,
                Err(err) => {
                    error!(
                        kind = "log_error_count",
                        network = &self.config.network.to_string(),
                    );
                    continue;
                }
            };

            #[cfg(feature = "postgres")]
            {
                event
                    .insert(
                        &self.pool,
                        &self.config.network.to_string(),
                        &log.core().tx_hash,
                    )
                    .await?;
            }

            #[cfg(feature = "redis-stream")]
            {
                let event = match_events(log);
                match self.queue_rx.send(Resource::Log(event)).await {
                    Ok(_) => {}
                    Err(err) => {
                        warn!(kind="propagate_error", network = &self.config.network.to_string(), err = ?err);
                    }
                }
            }
        }

        Ok(())
    }
```

Large prerequisite is acquiring a list of all the deployed pools as that's necessary to apply correct filter criteria (and prevent yourself from drowning in logs (especially in some layer-2s)).

The whole process can be illustrated with the diagram below.
{% mermaid() %}
graph TD
A[Node ETL] --> B[Manager]
B --> C1[Collector 1]
B --> C2[Collector 2]
B --> C3[Collector N]

    C1 --> D1[Node Instance]
    C1 --> D2[Database Pool]
    C1 --> D3[Redis Stream]

    I[Raw Events] <-->|Listen| D1
    D1 -->|Aggregate & Transform| J[Processed Events]
    J -->|Insert| D2[Database Pool]
    J -->|Send| D3[Redis Stream]

    subgraph "Collector"
    D1
    D2
    D3
    J
    end

    subgraph "Events"
    I
    end

    classDef manager fill:#f9f,stroke:#333,stroke-width:2px;
    classDef collector fill:#bbf,stroke:#333,stroke-width:2px;
    classDef process fill:#bfb,stroke:#333,stroke-width:2px;
    class B manager;
    class C1,C2,C3 collector;
    class I,J process;

{% end %}

<div style="text-align: center; font-style: italic; margin-top: 10px;">Node ETL overview</div>

# Websocket Gateway

The websocket gateway complements the Node ETL by handling client interactions, including authentication, topic subscriptions, and real-time event delivery. The server is utilising the [actor model](https://en.wikipedia.org/wiki/Actor_model) to segregate the responsibilities between the different logical components.

The lifecycle of a client establishing a connection is roughly summarised by the below diagram. Please note that the diagram represents happy-path scenarios only.
{% mermaid() %}
sequenceDiagram
actor C as Client[Entity]
participant SES as ClientSession[Actor]
participant DB as Database[Actor]
participant SRV as Server[Actor]
participant DEFI as DEFI[Actor]
participant RED as Redis

    C ->> SES: Begin Handshake
    C ->> SES: Send an API Key
    SES ->> DB: Verify the API Key
    SES ->> SRV: Add the new client to the list of clients with established sessions
    SRV ->> DB: Acquire client's relevant information (used credits/plan details etc.)
    SRV ->> C: Establish connection
    C ->> SES: Subscribe to specific topics
    SES ->> SRV: Verify the correctness of the message

    loop Continuous Communication
        DEFI ->> RED: Poll for new events
        RED -->> DEFI: Return new events
        DEFI -->> DB: Optionally log the events before propagating
        DEFI -->> SRV: Send synthesised events for further client distribution
    end

    SRV ->> SES: Send a msg to all client sessions each time a relevant event happens
    SRV ->> SRV: Modify the user's used credits
    SES ->> C: ClientSession actor sends the messages to all end client entities

    rect rgb(240, 240, 240)
        Note over C,SRV: Session End
        C ->> SES: Close connection
        SES ->> SRV: Notify of client disconnect
        SES ->> SES: Cleanup client-specific resources
        SRV ->> DB: Update client's session data (total time, credits used, etc.)
        SRV ->> SRV: Remove client from active subscribers list
    end

{% end %}

The authentication happens through the `Authorization` header - as recommended by the [Websocket RFC](https://www.rfc-editor.org/rfc/rfc6455.html) (more on how to authenticate can be found in the [official docs](https://docs.polia.io/#authentication)).

All messages are parsed to determine the networks and dexes of interest of the one connecting. Clients can subscribe to as many networks and/or dexes as they're interested in - so long as they can handle the amount of data.

```rust
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for ClientSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Text(txt)) => {
                match serde_json::from_str::<ClientPayload>(&txt) {
                    Ok(payload) => {
                        let information = self.information.clone();
                        let networks = parse_comma_separated_values(payload.networks);
                        let dex = parse_comma_separated_values(payload.dex);

                        match payload.method {
                            Method::Subscribe => {
                                self.server_addr.do_send(Subscribe {
                                    client_id: self.id,
                                    information: information.clone(),
                                    networks: networks.clone(),
                                    kind: payload.kind.unwrap_or_default(),
                                    dex: dex.clone(),
                                });
                            }
                            Method::Unsubscribe => {
                                self.server_addr.do_send(Unsubscribe {
                                    client_id: self.id,
                                    information: information.clone(),
                                    networks: networks.clone(),
                                    kind: payload.kind.unwrap_or_default(),
                                    dex: dex.clone(),
                                });
                            }
                        }
                    }
                    Err(_) => {
                        ctx.text(
                            json!({
                                "error": "Invalid Format. Please visit https://docs.polia.io for more information on how to format messages. Support is available at support@polia.io."
                            }).to_string()
                        );
                    }
                }
            }
            Ok(ws::Message::Binary(_)) => {}
            _ => (),
        }
    }
}
```

As previously outlined, the ETL-processed logs are propagated to an append-only stream in Redis. This allows the `DEFI` actor to maintain an event loop with connection to Redis, thereby accessing the newest events from the latest block. Running the `node ETL` as a standalone process, rather than directly integrating its responsibilities into the `websocket-gateway`, offers several advantages:

1. It enables easy scaling of the number of websocket instances running simultaneously (redis's append-only logs can be consumed by multiple entities).
2. It prevents an exponential increase in node event consumption. This is particularly crucial when using an RPC provider like [Infura](https://infura.io), especially when integrating multiple networks. While running dedicated nodes can alleviate this to some extent, relying solely on external RPC providers for multi-network integration can lead to significant consumption-related challenges.
3. The setup allows for the utilisation of real-time events by processes that offer additional functionality and flexibility.
