use std::{net::{IpAddr, SocketAddr}, path::PathBuf, sync::Arc};

use marcidb_http::{ServerContext, list_databases, serve};
use tokio::{fs, net::TcpListener};

/// Resolved server configuration. Precedence: CLI flag > environment variable > built-in default.
struct Config {
    host: IpAddr,
    port: u16,
    data_dir: String,
    token: Option<String>,
}

/// Parses CLI flags layered over environment variables over defaults. Accepts `--flag value` and
/// `--flag=value`. `--help`/`--version` print and exit; an unknown flag or a bad value exits with code 2.
fn parse_config() -> Config {
    // seed from the environment; CLI flags override below
    let mut host = std::env::var("MARCI_HOST").ok();
    let mut port = std::env::var("PORT").ok();
    let mut data = std::env::var("MARCI_DATA").ok();
    let mut token = std::env::var("MARCI_TOKEN").ok().filter(|t| !t.is_empty());

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) => (f.to_string(), Some(v.to_string())),
            None => (arg.clone(), None),
        };
        match flag.as_str() {
            "-h" | "--help" => { print_help(); std::process::exit(0); }
            "-v" | "--version" => { println!("marcidb-server {}", env!("CARGO_PKG_VERSION")); std::process::exit(0); }
            "--host" => host = Some(take_value(&flag, inline, &mut args)),
            "--port" => port = Some(take_value(&flag, inline, &mut args)),
            "--data" | "--data-dir" => data = Some(take_value(&flag, inline, &mut args)),
            "--token" => token = Some(take_value(&flag, inline, &mut args)).filter(|t| !t.is_empty()),
            other => {
                eprintln!("marcidb-server: unknown argument '{}'. Try '--help'.", other);
                std::process::exit(2);
            }
        }
    }

    let host = parse_host(host.as_deref().unwrap_or("0.0.0.0"));
    let port = match port {
        Some(p) => p.parse::<u16>().unwrap_or_else(|_| {
            eprintln!("marcidb-server: invalid port '{}' (expected 0-65535)", p);
            std::process::exit(2);
        }),
        None => 3000,
    };
    let data_dir = data.unwrap_or_else(|| "./data".to_string());

    Config { host, port, data_dir, token }
}

/// Pulls a flag's value from the inline `=value` or the next argument; exits 2 if absent.
fn take_value(flag: &str, inline: Option<String>, args: &mut impl Iterator<Item = String>) -> String {
    inline.or_else(|| args.next()).unwrap_or_else(|| {
        eprintln!("marcidb-server: missing value for '{}'. Try '--help'.", flag);
        std::process::exit(2);
    })
}

/// Bind host as an IP. `localhost` is accepted as a convenience for `127.0.0.1`.
fn parse_host(s: &str) -> IpAddr {
    if s.eq_ignore_ascii_case("localhost") {
        return IpAddr::from([127, 0, 0, 1]);
    }
    s.parse::<IpAddr>().unwrap_or_else(|_| {
        eprintln!("marcidb-server: invalid host '{}' (expected an IP such as 0.0.0.0, 127.0.0.1, or ::)", s);
        std::process::exit(2);
    })
}

fn print_help() {
    println!("MarciDB Server v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("USAGE:");
    println!("  marcidb-server [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("  --host <HOST>   Address to bind     [default: 0.0.0.0]  [env: MARCI_HOST]");
    println!("  --port <PORT>   Port to listen on   [default: 3000]     [env: PORT]");
    println!("  --data <DIR>    Data directory      [default: ./data]   [env: MARCI_DATA]");
    println!("  --token <TOKEN> Require 'Authorization: Bearer <TOKEN>' on every request  [env: MARCI_TOKEN]");
    println!("  -h, --help      Print help and exit");
    println!("  -v, --version   Print version and exit");
    println!();
    println!("CLI flags take precedence over environment variables.");
}

#[tokio::main]
async fn main() {
    let cfg = parse_config();

    if let Err(e) = fs::create_dir_all(&cfg.data_dir).await {
        eprintln!("MarciDB Server: cannot create data dir '{}': {}", cfg.data_dir, e);
        std::process::exit(1);
    }
    let root = PathBuf::from(&cfg.data_dir);
    let ctx: Arc<ServerContext> = Arc::new(ServerContext::directory(root.clone(), cfg.token.clone()));

    let addr = SocketAddr::new(cfg.host, cfg.port);
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("MarciDB Server: failed to bind {} ({}). Is the port already in use?", addr, e);
            std::process::exit(1);
        }
    };

    print_banner(&addr, &root, &list_databases(&root), cfg.token.is_some());

    serve(listener, ctx, async {
        let _ = tokio::signal::ctrl_c().await;
        println!("\n  Shutting down MarciDB Server. Bye!");
    }).await;
}

/// Startup banner: version, where to reach it, and what's already on disk
fn print_banner(addr: &SocketAddr, data_dir: &std::path::Path, dbs: &[String], auth: bool) {
    println!();
    println!("  MarciDB Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  ----------------------------------------");
    // 0.0.0.0 / :: bind to every interface — show the friendly localhost URL plus the wildcard
    if addr.ip().is_unspecified() {
        println!("  Local      http://localhost:{}", addr.port());
        println!("  Network    http://{}", addr);
    } else {
        println!("  Listening  http://{}", addr);
    }
    println!("  Data dir   {}", display_path(data_dir));
    println!("  Auth       {}", if auth { "bearer token required" } else { "open (set MARCI_TOKEN to require a token)" });
    if dbs.is_empty() {
        println!("  Databases  none yet (created on first migration)");
    } else {
        println!("  Databases  {} ({})", dbs.len(), dbs.join(", "));
    }
    println!();
    println!("  Ready. Press Ctrl+C to stop.");
    println!();
}


/// Absolute path for display, stripped of Windows' `\\?\` extended-length prefix
fn display_path(p: &std::path::Path) -> String {
    let s = std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf()).display().to_string();
    s.strip_prefix(r"\\?\").map(str::to_string).unwrap_or(s)
}