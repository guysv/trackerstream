//! Headless interop test for the embedded rust-ipfs client: connect to a kubo
//! master that pinned a module DAG, fetch the root over Bitswap, reassemble, and
//! assert the bytes are byte-identical to the original file. Proves rust-ipfs <->
//! kubo interop + the Rust reassembler. Driven by test/rust-interop.sh.
//!
//!   MASTER_ADDR=/ip4/127.0.0.1/tcp/PORT/p2p/PEERID ROOT_CID=bafy... \
//!   ORIG_FILE=/path/to/module  cargo run --example fetch-check
use desktop_lib::ipfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let master = std::env::var("MASTER_ADDR").expect("MASTER_ADDR");
    let root = std::env::var("ROOT_CID").expect("ROOT_CID");
    let orig_path = std::env::var("ORIG_FILE").ok(); // optional: byte-exact compare

    let node = ipfs::start(None).await?;
    println!("client peer: {}", node.peer_id);
    ipfs::connect(&node.ipfs, &master).await?;
    println!("connected to master: {master}");

    let root_cid = root.parse()?;
    let t0 = std::time::Instant::now();
    let bytes = ipfs::reassemble(&node.ipfs, root_cid).await?;
    let ms = t0.elapsed().as_millis();

    let ok = match &orig_path {
        Some(p) => {
            let orig = std::fs::read(p)?;
            let eq = bytes == orig;
            println!(
                "{} reassembled {} bytes in {ms}ms (orig {} bytes) — {}",
                if eq { "OK  " } else { "FAIL" },
                bytes.len(),
                orig.len(),
                if eq { "BYTE-EXACT over libp2p" } else { "DIFFER" }
            );
            eq
        }
        None => {
            // No original to diff: every block was CID-verified during fetch, so a
            // successful reassembly already proves integrity.
            println!("OK   reassembled {} bytes in {ms}ms from CID blocks over libp2p (every block CID-verified)", bytes.len());
            true
        }
    };
    std::process::exit(if ok { 0 } else { 1 });
}
