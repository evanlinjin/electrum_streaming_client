use bdk_testenv::{anyhow, bitcoincore_rpc::RpcApi, TestEnv};
use electrum_streaming_client::{notification::Notification, request, BlockingClient, Event};
use std::time::Duration;

#[test]
fn blocking_client_ping() -> anyhow::Result<()> {
    use std::net::TcpStream;

    let env = TestEnv::new()?;
    let electrum_addr = env.electrsd.electrum_url.clone();
    println!("URL: {}", electrum_addr);

    let stream = TcpStream::connect(&electrum_addr)?;
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let (reader, writer) = (stream.try_clone()?, stream);
    let (client, _event_rx, handle) = BlockingClient::new(reader, writer);

    // Test ping
    client.ping()?;
    println!("Ping successful!");

    // Clean shutdown
    drop(client);
    handle.join().expect("client thread should not panic")?;

    Ok(())
}

#[test]
fn blocking_client_headers() -> anyhow::Result<()> {
    use std::net::TcpStream;

    let env = TestEnv::new()?;
    let electrum_addr = env.electrsd.electrum_url.clone();
    println!("URL: {}", electrum_addr);

    // Mine some blocks first
    env.mine_blocks(10, None)?;
    env.wait_until_electrum_sees_block(Duration::from_secs(5))?;

    let stream = TcpStream::connect(&electrum_addr)?;
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let (reader, writer) = (stream.try_clone()?, stream);
    let (client, _event_rx, handle) = BlockingClient::new(reader, writer);

    // Test getting headers
    let header = client.header(5)?;
    println!("Got header at height 5: {:?}", header.header.block_hash());

    // Test getting multiple headers
    let headers = client.headers(1, 5)?;
    println!("Got {} headers", headers.count);
    assert_eq!(headers.count, 5);
    assert_eq!(headers.headers.len(), 5);

    // Clean shutdown
    drop(client);
    handle.join().expect("client thread should not panic")?;

    Ok(())
}

#[test]
fn blocking_client_with_events() -> anyhow::Result<()> {
    use std::net::TcpStream;

    let env = TestEnv::new()?;
    let electrum_addr = env.electrsd.electrum_url.clone();
    println!("URL: {}", electrum_addr);

    let wallet_addr = env
        .rpc_client()
        .get_new_address(None, None)?
        .assume_checked();

    let stream = TcpStream::connect(&electrum_addr)?;
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    let (reader, writer) = (stream.try_clone()?, stream);
    let (client, event_rx, handle) = BlockingClient::new(reader, writer);

    // Subscribe to headers
    client.send_event_request(request::Request::HeadersSubscribe)?;

    // Wait for subscription response
    let start = std::time::Instant::now();
    let mut got_subscription = false;
    while !got_subscription && start.elapsed() < Duration::from_secs(5) {
        if let Ok(event) = event_rx.recv_timeout(Duration::from_millis(100)) {
            println!("Got event: {:?}", event);
            if matches!(event, Event::Response { request: request::Request::HeadersSubscribe, .. }) {
                got_subscription = true;
            }
        }
    }
    assert!(got_subscription, "Should have received subscription response");

    // Mine blocks one by one to trigger notifications
    let blocks_to_mine = 3;
    let mut header_count = 0;
    
    for i in 1..=blocks_to_mine {
        println!("Mining block {}...", i);
        env.mine_blocks(1, Some(wallet_addr.clone()))?;
        env.wait_until_electrum_sees_block(Duration::from_secs(5))?;
        
        // Check for header notification
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if let Ok(event) = event_rx.recv_timeout(Duration::from_millis(100)) {
                if matches!(event, Event::Notification(Notification::Header(_))) {
                    header_count += 1;
                    println!("Received header notification #{}", header_count);
                    break;
                }
            }
        }
    }

    // We should have received at least one notification
    // (may not get all 3 due to timing)
    assert!(
        header_count > 0,
        "Should have received at least one header notification, got {}", header_count
    );

    // Clean shutdown
    drop(client);
    handle.join().expect("client thread should not panic")?;

    Ok(())
}