use electrum_streaming_client::BlockingClient;
use std::net::TcpStream;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to a public Electrum server (blockstream.info)
    let stream = TcpStream::connect("blockstream.info:110")?;
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    
    let (reader, writer) = (stream.try_clone()?, stream);
    let (client, _event_rx, _handle) = BlockingClient::new(reader, writer);
    
    // Test ping
    println!("Sending ping...");
    client.ping()?;
    println!("Ping successful!");
    
    // Test getting relay fee
    println!("Getting relay fee...");
    let relay_fee = client.relay_fee()?;
    println!("Relay fee: {:?}", relay_fee.fee);
    
    // Test getting a banner
    println!("Getting server banner...");
    let banner = client.banner()?;
    println!("Server banner: {}", banner);
    
    // Test getting a header
    println!("Getting header at height 100000...");
    let header = client.header(100000)?;
    println!("Block hash at height 100000: {}", header.header.block_hash());
    
    Ok(())
}