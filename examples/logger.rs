#[macro_use] extern crate cocaine;

use std::thread;
use std::time::Duration;

use cocaine::logging::{Sev, LoggerContext};

fn main() {
    let ctx = LoggerContext::new();
    let log = ctx.create("proxy/access");

    loop {
        // The simpliest message.
        log!(log, Sev::Info, "nginx/1.6 configured");

        // Using lazy format arguments.
        log!(log, Sev::Info, "{} {} HTTP/1.1 {} {}", "GET", "/static/image.png", 404, 347);

        // Attaching additional meta information.
        log!(log, Sev::Info, "nginx/1.6 configured", {
            config: "/etc/nginx/nginx.conf",
            elapsed: 42.15,
        });

        // More ...
        log!(log, Sev::Warn, "client stopped connection before send body completed", {
            host: "::1",
            port: 10053,
        });

        // And both. You can even use functions as meta for lazy evaluations.
        log!(log, Sev::Error, "file does not exist: {}", ["/var/www/favicon.ico"], {
            path: "/",
            cache: true,
            method: "GET",
            version: 1.1,
            protocol: "HTTP",
        });

        thread::sleep(Duration::new(0, 1000000));
    }
}
