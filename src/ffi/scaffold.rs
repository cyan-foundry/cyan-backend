use crate::DATA_DIR;
use std::ffi::{c_char, CStr, CString};

pub fn compute_or_load_node_id() -> String {
    if let Some(dir) = DATA_DIR.get() {
        let node_id_file = dir.join("node_id.txt");
        if let Ok(id) = std::fs::read_to_string(&node_id_file) {
            return id.trim().to_string();
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    save_node_id_to_disk(&id);
    id
}

pub fn save_node_id_to_disk(id: &str) {
    if let Some(dir) = DATA_DIR.get() {
        let node_id_file = dir.join("node_id.txt");
        let _ = std::fs::write(node_id_file, id);
    }
}

pub unsafe fn cstr_arg(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        CStr::from_ptr(ptr).to_str().ok().map(|s| s.to_string())
    }
}

pub fn to_c_string(s: String) -> *const c_char {
    CString::new(s).unwrap_or_default().into_raw()
}
