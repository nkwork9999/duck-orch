// OpenLineage HTTP emitter. Background worker thread + bounded queue.
//
// Compatible with duck_lineage so Marquez/DataHub can consume both event streams.

use once_cell::sync::Lazy;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

const QUEUE_CAPACITY: usize = 10_000;

struct Config {
    url: Option<String>,
    api_key: Option<String>,
    debug: bool,
}

static CONFIG: Lazy<Mutex<Config>> = Lazy::new(|| {
    Mutex::new(Config {
        url: None,
        api_key: None,
        debug: false,
    })
});

static SENDER: Lazy<SyncSender<String>> = Lazy::new(|| {
    let (tx, rx) = sync_channel::<String>(QUEUE_CAPACITY);
    thread::spawn(move || {
        for event in rx.iter() {
            send_event(&event);
        }
    });
    tx
});

pub fn set_url(url: &str) {
    if let Ok(mut c) = CONFIG.lock() {
        c.url = if url.is_empty() { None } else { Some(url.to_string()) };
    }
}

pub fn set_api_key(key: &str) {
    if let Ok(mut c) = CONFIG.lock() {
        c.api_key = if key.is_empty() { None } else { Some(key.to_string()) };
    }
}

pub fn set_debug(d: bool) {
    if let Ok(mut c) = CONFIG.lock() {
        c.debug = d;
    }
}

pub fn enqueue(event_json: &str) -> bool {
    let url_set = match CONFIG.lock() {
        Ok(c) => c.url.is_some(),
        Err(_) => false,
    };
    if !url_set {
        return false;
    }
    SENDER.try_send(event_json.to_string()).is_ok()
}

fn send_event(event: &str) {
    let (url, api_key, debug) = match CONFIG.lock() {
        Ok(c) => (c.url.clone(), c.api_key.clone(), c.debug),
        Err(_) => return,
    };
    let url = match url {
        Some(u) => u,
        None => return,
    };
    if debug {
        eprintln!("[duckorch OL] -> {}: {}", url, event);
    }
    // Try a few times with exponential backoff
    let mut attempt = 0;
    loop {
        let mut req = ureq::post(&url)
            .timeout(Duration::from_secs(10))
            .set("Content-Type", "application/json");
        if let Some(k) = &api_key {
            req = req.set("Authorization", &format!("Bearer {}", k));
        }
        match req.send_string(event) {
            Ok(_) => return,
            Err(e) => {
                if debug {
                    eprintln!("[duckorch OL] attempt {} failed: {}", attempt, e);
                }
                attempt += 1;
                if attempt >= 3 {
                    return;
                }
                thread::sleep(Duration::from_millis(200 * (1 << attempt)));
            }
        }
    }
}
