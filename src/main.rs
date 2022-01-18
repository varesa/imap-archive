#[macro_use]
extern crate lazy_static;

use std::env;
use imap::Session;
use native_tls::TlsStream;
use std::net::TcpStream;
use std::collections::HashMap;
use std::sync::Mutex;


const MAX_UIDS: usize = 256;

type Uid = u32;
type Year = u32;

///
/// Turn a HashSet with UIDs into a comma separated String
///
fn create_uidset(uids: &Vec<u32>) -> String {
    uids.iter()
        .map(|uid| uid.to_string() )
        .fold(String::new(), |mut a, b| { 
            if !a.is_empty() {
                a.push(',');
            }
            a.push_str(&b); 
            a 
        }
    )
}

fn year_to_folder(year: Year) -> String {
    String::from("Archives/") + &year.to_string()
}

lazy_static! {
    static ref EXISTING_YEARS: Mutex<Vec<Year>> = Mutex::new(Vec::new());
}

///
/// Ensure that a mail folder exists for the given year. Checks first
/// if the folder already exists and caches the result so that at the most
/// the server will see one LIST and one CREATE per folder.
///
fn create_folder(year: &Year, session: &mut Session<TlsStream<TcpStream>>) {
    let mut cached = EXISTING_YEARS.lock().unwrap();
    if cached.contains(year) {
        // We have already tested/created this year
        return;
    }

    let folder_name = year_to_folder(*year);
    let folders = session.list(None, Some(&folder_name)).unwrap();
    assert!(folders.len() < 2);

    if !folders.is_empty() {
        println!("Caching existing folder for year {year}");
        cached.push(*year);
        return;
    }

    println!("Creating missing folder for year {year}");
    session.create(folder_name).unwrap();
    cached.push(*year);
}

fn archive_messages(year: Year, uids: &Vec<Uid>, session: &mut Session<TlsStream<TcpStream>>) {
    let uidset = create_uidset(uids);
    let folder_name = year_to_folder(year);

    session.uid_mv(uidset, folder_name).unwrap();
}

///
/// Take a batch of messages and archive them
///
fn process_messages(uids: Vec<Uid>, session: &mut Session<TlsStream<TcpStream>>) {
    println!("Processing {} messages", uids.len());
    let uidset = create_uidset(&uids);
    let messages = session.uid_fetch(uidset, "(UID INTERNALDATE)").unwrap();

    let mut years = HashMap::<Year, Vec<Uid>>::new();
    for message in messages.iter() {
        let year = message.internal_date().unwrap().format("%Y").to_string().parse::<Year>().unwrap();
        years.entry(year).or_insert(Vec::new());
        years.get_mut(&year).unwrap().push(message.uid.unwrap());
    }

    for year in years.keys() {
        create_folder(year, session);
        archive_messages(*year, &years[year], session);
    }
}


fn main() {
    let args: Vec<String> = env::args().collect();
    let server = args[1].clone();
    let server: &str = server.as_str();

    let username =
        env::var("IMAP_USERNAME").expect("Missing or invalid env var: IMAP_USERNAME");
    let password =
        env::var("IMAP_PASSWORD").expect("Missing or invalid env var: IMAP_PASSWORD");

    let tls = native_tls::TlsConnector::builder().build().unwrap();
    let client = imap::connect_starttls((server, 143), server, &tls).unwrap();

    let mut session = client.login(username, password).unwrap();

    let capabilities = session.capabilities().unwrap();
    assert!(capabilities.has_str("MOVE"));

    let mailbox = session.select("INBOX").unwrap();
    assert!(mailbox.uid_validity.is_some());

    let uids = session.uid_search("ALL").unwrap();

    let mut batch: Vec<Uid> = Vec::new();
    for uid in uids.iter() {
        batch.push(*uid);
        if batch.len() == MAX_UIDS {
            process_messages(batch, &mut session);
            batch = Vec::new();
        }
    }
    if !batch.is_empty() {
        process_messages(batch, &mut session);
    }

}
