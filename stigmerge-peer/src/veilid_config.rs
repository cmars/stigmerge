use std::env;

use veilid_core::VeilidConfig;

pub fn node_addr() -> Option<String> {
    match env::var("NODE_ADDR") {
        Ok(val) => Some(val),
        Err(_) => None,
    }
}

pub fn get_config(state_dir: String, ns: Option<String>) -> VeilidConfig {
    return VeilidConfig {
        program_name: "stigmerge".into(),
        namespace: ns.unwrap_or_default(),
        table_store: veilid_core::VeilidConfigTableStore {
            directory: get_table_store_path(&state_dir),
            ..Default::default()
        },
        block_store: veilid_core::VeilidConfigBlockStore {
            directory: get_block_store_path(&state_dir),
            ..Default::default()
        },
        protected_store: veilid_core::VeilidConfigProtectedStore {
            allow_insecure_fallback: true,
            always_use_insecure_storage: always_use_insecure_storage(),
            directory: get_protected_store_path(&state_dir),
            ..Default::default()
        },
        ..Default::default()
    };
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
