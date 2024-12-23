use std::thread;
use std::time::Duration;
use clap::{Arg, Command};
use ::bsread::*;
use ::bsread::dispatcher::ChannelDescription;

fn on_message(message: &Message, level:u64, size:usize) -> () {
    let print_main_header = level>3;
    let print_data_header = level>3;
    let print_meta = level==2;
    let print_data = level>=3;
    debug::print_message( &message, size, print_main_header,
                          print_data_header, print_meta, print_data);
}

struct Context {
    receiver: Receiver,
    level:u64,
    size:usize,
    debug:bool
}

impl Context {

    fn new(endpoint: &str, socket_type: SocketType, level: Option<u64>, size: Option<usize>, debug:bool) -> IOResult<Self> {
        if debug {

            debug::start_sender(10300, if socket_type == SocketType::PULL {SocketType::PUSH} else {SocketType::PUB}, 100, None, None).expect("Failed to start sender");
        }
        let bsread =  Bsread::new().expect("Failed to open bsread");
        let receiver = bsread.receiver(Some(vec![endpoint]),socket_type).expect("Failed to create receiver");
        Ok(Self {receiver, level : match level{None => {3} Some(v) => {v}}, size : match size{None => {10}Some(v) => {v}},debug})
    }

    fn start(&mut self, num_messages: Option<u32>, time: Option<i64>, ) -> IOResult<()> {
        let print_level = self.level;
        let print_size = self.size;
        self.receiver.fork(move |msg| {on_message(&msg, print_level, print_size)}, num_messages);
        if let Some(tm) = time {
            thread::sleep(Duration::from_millis(if tm>0  {tm as u64} else {u64::MAX}));
        }
        if let Some(_m) = num_messages {
            self.receiver.join()?;
        }
        Ok(())
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        self.receiver.interrupt();
        self.receiver.join();
        if self.level>0{
            debug::print_stats_rec(&self.receiver);
            }
        if self.debug {
            debug::stop_senders();
        }
    }
}
#[macro_export]
macro_rules! exit {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(1);
    }};
}

fn main(){
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).try_init();

    let matches = Command::new("BsReceiver")
        .version("1.0")
        .author("Alexandre Gobbo <alexandre.gobbo@psi.ch>")
        .about("Test and print Bsread streams")
        .arg(
            Arg::new("Time")
                .short('t')
                .long("time")
                .value_name("TIME")
                .help("Running time in ms")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Messages")
                .short('m')
                .long("messages")
                .value_name("MESSAGES")
                .help("Number of messages (Default=1)")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Pull")
                .short('p')
                .long("pull")
                .help("Creates socket with type PULL  (Default=SUB)")
                .num_args(0), // Does not take a value
        )
        .arg(
            Arg::new("Debug")
                .short('d')
                .long("debug")
                .help("Creates a debug sender on port 10300 and sets the endpoint")
                .num_args(0), // Does not take a value
        )
        .arg(
            Arg::new("Level")
                .short('l')
                .long("level")
                .value_name("LEVEL")
                .help("Print level: 0 to 4 (Default=3)")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Size")
                .short('s')
                .long("size")
                .value_name("SIZE")
                .help("Maximum array print size (Default=10)")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Url")
                .short('u')
                .long("url")
                .value_name("URL")
                .help("Source endpoint")
                .num_args(1) // Expects one value
                .required(false),
        )
        .arg(
            Arg::new("Channel")
                .short('c')
                .long("channel")
                .value_name("CHANNEL")
                .help("Dispatcher channel name")
                .action(clap::ArgAction::Append) // Collect all values into a Vec<String>
                .required(false),
        )
        .get_matches();

    // Check if the help flag is present
    if matches.contains_id("help") {
        exit!("Error: Use -h or --help for instructions.");
    }

    // Check if the boolean flag is present
    let socket_type = if matches.get_flag("Pull") {
        SocketType::PULL
    }   else {
        SocketType::SUB
    };

    let debug = if matches.get_flag("Debug") {
        true
    }   else {
        false
    };

    let time =if let Some(text) = matches.get_one::<String>("Time") {
        match text.parse::<i64>() {
            Ok(number) => Some(number),
            Err(_) => {exit!("Invalid value of time: {}", text);},
        }
    } else {
        None
    };

    let mut messages =if let Some(text) = matches.get_one::<String>("Messages") {
        match text.parse::<u32>() {
            Ok(number) => Some(number),
            Err(_) => {exit!("Invalid value of messages: {}", text);},
        }
    } else {
        None
    };

    // Parse the numeric argument
    let level =if let Some(text) = matches.get_one::<String>("Level") {
        match text.parse::<u64>() {
            Ok(number) => Some(number),
            Err(_) => {exit!("Invalid value of level: {}", text);},
        }
    } else {
        None
    };

    let size =if let Some(text) = matches.get_one::<String>("Size") {
        match text.parse::<usize>() {
            Ok(number) => Some(number),
            Err(_) => {exit!("Invalid value of size: {}", text);},
        }
    } else {
        None
    };

    let url = matches.get_one::<String>("Url");
    let channels: Vec<String> = matches
        .get_many::<String>("Channel")
        .unwrap_or_default()
        .cloned()
        .collect();

    if ! debug {
        if url.is_none() && channels.is_empty() {
            exit!("Either URL (-u, --url) or channel names (-c, --channel) must me defined");
        }
    }
    let endpoint = match url {
        None => {
            if debug{
                "tcp://127.0.0.1:10300".to_string()
            } else {
                let mut descriptions: Vec<ChannelDescription> = Vec::new();
                for channel in channels {
                    descriptions.push(ChannelDescription::of(channel.as_str()));
                }
                match dispatcher::request_stream(descriptions, None, None, true, false) {
                    Ok(ds) => ds.get_endpoint().to_owned(), // Clone or take ownership here
                    Err(e) => exit!("Error requesting stream to Dispatcher: {}", e),
                }
            }
        }
        Some(url) => url.to_string(),
    };
    if messages.is_none() && time.is_none(){
        messages=Some(1);
    }


    let mut context = Context::new(endpoint.as_str(), socket_type, level, size, debug).expect("Failed to create context");
    context.start(messages, time);
}
