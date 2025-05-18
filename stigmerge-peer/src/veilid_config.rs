use std::{env, str::FromStr};

use veilid_core::{
    ConfigCallbackReturn, FourCC, TypedKey, TypedKeyGroup, TypedSecretGroup, VeilidAPIError,
};

pub fn node_addr() -> Option<String> {
    match env::var("NODE_ADDR") {
        Ok(val) => Some(val),
        Err(_) => None,
    }
}

pub fn callback(state_dir: String, ns: Option<String>, key: String) -> ConfigCallbackReturn {
    match key.as_str() {
        "program_name" => Ok(Box::new(String::from("stigmerge"))),
        "namespace" => Ok(if let Some(name) = ns {
            Box::new(name)
        } else {
            Box::<String>::default()
        }),
        "capabilities.disable" => Ok(Box::<Vec<FourCC>>::default()),
        "table_store.directory" => Ok(Box::new(get_table_store_path(&state_dir))),
        "table_store.delete" => Ok(Box::new(false)),
        "block_store.directory" => Ok(Box::new(get_block_store_path(&state_dir))),
        "block_store.delete" => Ok(Box::new(false)),
        "protected_store.allow_insecure_fallback" => Ok(Box::new(true)),
        "protected_store.always_use_insecure_storage" => {
            Ok(Box::new(always_use_insecure_storage()))
        }
        "protected_store.directory" => Ok(Box::new(get_protected_store_path(&state_dir))),
        "protected_store.delete" => Ok(Box::new(false)),
        "protected_store.device_encryption_key_password" => Ok(Box::new("".to_owned())),
        "protected_store.new_device_encryption_key_password" => {
            Ok(Box::new(Option::<String>::None))
        }
        "network.connection_initial_timeout_ms" => Ok(Box::new(2_000u32)),
        "network.connection_inactivity_timeout_ms" => Ok(Box::new(60_000u32)),
        "network.dht.member_watch_limit" => Ok(Box::new(8u32)),
        "network.dht.public_watch_limit" => Ok(Box::new(32u32)),
        "network.max_connections_per_ip4" => Ok(Box::new(8u32)),
        "network.max_connections_per_ip6_prefix" => Ok(Box::new(8u32)),
        "network.max_connections_per_ip6_prefix_size" => Ok(Box::new(56u32)),
        "network.max_connection_frequency_per_min" => Ok(Box::new(8u32)),
        "network.client_allowlist_timeout_ms" => Ok(Box::new(300_000u32)),
        "network.reverse_connection_receipt_time_ms" => Ok(Box::new(5_000u32)),
        "network.hole_punch_receipt_time_ms" => Ok(Box::new(5_000u32)),
        "network.network_key_password" => Ok(Box::new(Option::<String>::None)),
        "network.routing_table.node_id" => Ok(Box::new(TypedKeyGroup::new())),
        "network.routing_table.node_id_secret" => Ok(Box::new(TypedSecretGroup::new())),
        "network.routing_table.bootstrap" => {
            Ok(Box::new(vec!["bootstrap-v1.veilid.net".to_string()]))
        }
        "network.routing_table.bootstrap_keys" => Ok(Box::new(vec![
            TypedKey::from_str("VLD0:Vj0lKDdUQXmQ5Ol1SZdlvXkBHUccBcQvGLN9vbLSI7k").unwrap(),
            TypedKey::from_str("VLD0:QeQJorqbXtC7v3OlynCZ_W3m76wGNeB5NTF81ypqHAo").unwrap(),
            TypedKey::from_str("VLD0:QNdcl-0OiFfYVj9331XVR6IqZ49NG-E18d5P7lwi4TA").unwrap(),
        ])),
        "network.routing_table.limit_over_attached" => Ok(Box::new(64u32)),
        "network.routing_table.limit_fully_attached" => Ok(Box::new(32u32)),
        "network.routing_table.limit_attached_strong" => Ok(Box::new(16u32)),
        "network.routing_table.limit_attached_good" => Ok(Box::new(8u32)),
        "network.routing_table.limit_attached_weak" => Ok(Box::new(4u32)),
        "network.rpc.concurrency" => Ok(Box::new(2u32)),
        "network.rpc.queue_size" => Ok(Box::new(1024u32)),
        "network.rpc.max_timestamp_behind_ms" => Ok(Box::new(Some(10_000u32))),
        "network.rpc.max_timestamp_ahead_ms" => Ok(Box::new(Some(10_000u32))),
        "network.rpc.timeout_ms" => Ok(Box::new(5_000u32)),
        "network.rpc.max_route_hop_count" => Ok(Box::new(4u8)),
        "network.rpc.default_route_hop_count" => Ok(Box::new(1u8)),
        "network.dht.max_find_node_count" => Ok(Box::new(20u32)),
        "network.dht.resolve_node_timeout_ms" => Ok(Box::new(10_000u32)),
        "network.dht.resolve_node_count" => Ok(Box::new(1u32)),
        "network.dht.resolve_node_fanout" => Ok(Box::new(4u32)),
        "network.dht.get_value_timeout_ms" => Ok(Box::new(10_000u32)),
        "network.dht.get_value_count" => Ok(Box::new(3u32)),
        "network.dht.get_value_fanout" => Ok(Box::new(4u32)),
        "network.dht.set_value_timeout_ms" => Ok(Box::new(10_000u32)),
        "network.dht.set_value_count" => Ok(Box::new(5u32)),
        "network.dht.set_value_fanout" => Ok(Box::new(4u32)),
        "network.dht.min_peer_count" => Ok(Box::new(20u32)),
        "network.dht.min_peer_refresh_time_ms" => Ok(Box::new(60_000u32)),
        "network.dht.validate_dial_info_receipt_time_ms" => Ok(Box::new(5_000u32)),
        "network.dht.local_subkey_cache_size" => Ok(Box::new(128u32)),
        "network.dht.local_max_subkey_cache_memory_mb" => Ok(Box::new(256u32)),
        "network.dht.max_watch_expiration_ms" => Ok(Box::new(600_000u32)),
        "network.dht.remote_subkey_cache_size" => Ok(Box::new(1024u32)),
        "network.dht.remote_max_records" => Ok(Box::new(4096u32)),
        "network.dht.remote_max_subkey_cache_memory_mb" => Ok(Box::new(64u32)),
        "network.dht.remote_max_storage_space_mb" => Ok(Box::new(64u32)),
        "network.upnp" => Ok(Box::new(true)),
        "network.detect_address_changes" => Ok(Box::new(true)),
        "network.restricted_nat_retries" => Ok(Box::new(3u32)),
        "network.tls.certificate_path" => Ok(Box::new(get_certfile_path(&state_dir))),
        "network.tls.private_key_path" => Ok(Box::new(get_keyfile_path(&state_dir))),
        "network.tls.connection_initial_timeout_ms" => Ok(Box::new(2_000u32)),
        "network.application.https.enabled" => Ok(Box::new(false)),
        "network.application.https.listen_address" => Ok(Box::new("".to_owned())),
        "network.application.https.path" => Ok(Box::new(String::from("app"))),
        "network.application.https.url" => Ok(Box::new(Option::<String>::None)),
        "network.application.http.enabled" => Ok(Box::new(false)),
        "network.application.http.listen_address" => Ok(Box::new("".to_owned())),
        "network.application.http.path" => Ok(Box::new(String::from("app"))),
        "network.application.http.url" => Ok(Box::new(Option::<String>::None)),
        "network.protocol.udp.enabled" => Ok(Box::new(true)),
        "network.protocol.udp.socket_pool_size" => Ok(Box::new(16u32)),
        "network.protocol.udp.listen_address" => Ok(Box::new(node_addr().unwrap_or("".to_owned()))),
        "network.protocol.udp.public_address" => Ok(Box::new(Option::<String>::None)),
        "network.protocol.tcp.connect" => Ok(Box::new(true)),
        "network.protocol.tcp.listen" => Ok(Box::new(true)),
        "network.protocol.tcp.max_connections" => Ok(Box::new(32u32)),
        "network.protocol.tcp.listen_address" => Ok(Box::new(node_addr().unwrap_or("".to_owned()))),
        "network.protocol.tcp.public_address" => Ok(Box::new(Option::<String>::None)),
        "network.protocol.ws.connect" => Ok(Box::new(false)),
        "network.protocol.ws.listen" => Ok(Box::new(false)),
        "network.protocol.ws.max_connections" => Ok(Box::new(16u32)),
        "network.protocol.ws.listen_address" => Ok(Box::new(node_addr().unwrap_or("".to_owned()))),
        "network.protocol.ws.path" => Ok(Box::new(String::from("ws"))),
        "network.protocol.ws.url" => Ok(Box::new(Option::<String>::None)),
        "network.protocol.wss.connect" => Ok(Box::new(false)),
        "network.protocol.wss.listen" => Ok(Box::new(false)),
        "network.protocol.wss.max_connections" => Ok(Box::new(16u32)),
        "network.protocol.wss.listen_address" => Ok(Box::new(node_addr().unwrap_or("".to_owned()))),
        "network.protocol.wss.path" => Ok(Box::new(String::from("ws"))),
        "network.protocol.wss.url" => Ok(Box::new(Option::<String>::None)),
        _ => {
            let err = format!("config key '{}' doesn't exist", key);
            println!("{}", err);
            Err(VeilidAPIError::internal(err))
        }
    }
}

#[cfg(not(target_os = "android"))]
fn always_use_insecure_storage() -> bool {
    false
}

#[cfg(target_os = "android")]
fn always_use_insecure_storage() -> bool {
    true
}

fn get_block_store_path(state_dir: &String) -> String {
    format!("{}/block", state_dir)
}

fn get_table_store_path(state_dir: &String) -> String {
    format!("{}/table", state_dir)
}

fn get_protected_store_path(state_dir: &String) -> String {
    format!("{}/protected", state_dir)
}

fn get_certfile_path(state_dir: &String) -> String {
    format!("{}/cert", state_dir)
}

fn get_keyfile_path(state_dir: &String) -> String {
    format!("{}/key", state_dir)
}
