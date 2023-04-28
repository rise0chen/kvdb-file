use kvdb::{DBKeyValue, DBOp, DBTransaction, DBValue, KeyValueDB};
use kvdb_memorydb::InMemory;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// A key-value database fulfilling the `KeyValueDB` trait, living in file.
/// This is generally intended for tests and is not particularly optimized.
#[derive(Default)]
pub struct InFile {
    path: String,
    in_memory: InMemory,
}
impl InFile {
    fn col_path(&self, col: u32) -> PathBuf {
        let mut path = PathBuf::from(&self.path);
        path.push(col.to_string());
        path
    }
    fn key2file(&self, col: u32, key: &[u8]) -> PathBuf {
        let mut path = PathBuf::from(&self.path);
        path.push(col.to_string());
        path.push(format!("0x{}", hex::encode(key)));
        path
    }
    fn file2key(path: &Path) -> Option<Vec<u8>> {
        if let Some(name) = path.file_name() {
            let name = name.to_string_lossy();
            if let Ok(key) = hex::decode(&name[2..]) {
                return Some(key);
            }
        }
        None
    }
    pub fn open<P: AsRef<Path>>(path: P, num_cols: u32) -> Result<InFile, io::Error> {
        let in_memory = kvdb_memorydb::create(num_cols);
        let mut txn = DBTransaction::new();
        for col in 0..num_cols {
            let col_dir = path.as_ref().join(col.to_string());
            fs::create_dir_all(&col_dir)?;
            for entry in fs::read_dir(col_dir)? {
                let file = entry?.path();
                if file.is_file() {
                    if let Some(key) = Self::file2key(&file) {
                        let value = fs::read(file)?;
                        txn.put_vec(col, &key, value);
                    }
                }
            }
        }

        in_memory.write(txn)?;
        Ok(InFile {
            path: path.as_ref().to_string_lossy().into_owned(),
            in_memory,
        })
    }
}

impl KeyValueDB for InFile {
    fn get(&self, col: u32, key: &[u8]) -> io::Result<Option<DBValue>> {
        self.in_memory.get(col, key)
    }

    fn get_by_prefix(&self, col: u32, prefix: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.in_memory.get_by_prefix(col, prefix)
    }

    fn write(&self, transaction: DBTransaction) -> io::Result<()> {
        for op in &transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => {
                    let file = self.key2file(*col, key);
                    fs::write(file, value)?;
                }
                DBOp::Delete { col, key } => {
                    let file = self.key2file(*col, key);
                    if file.is_file() {
                        fs::remove_file(file)?;
                    }
                }
                DBOp::DeletePrefix { col, prefix } => {
                    let col_dir = self.col_path(*col);
                    if prefix.is_empty() {
                        for entry in fs::read_dir(col_dir)? {
                            let file = entry?.path();
                            if file.is_file() {
                                fs::remove_file(file)?;
                            }
                        }
                    } else {
                        for entry in fs::read_dir(col_dir)? {
                            let file = entry?.path();
                            if file.is_file() {
                                if let Some(key) = Self::file2key(&file) {
                                    if key.starts_with(&prefix) {
                                        fs::remove_file(file)?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.in_memory.write(transaction)
    }

    // NOTE: clones the whole db
    fn iter<'a>(&'a self, col: u32) -> Box<dyn Iterator<Item = io::Result<DBKeyValue>> + 'a> {
        self.in_memory.iter(col)
    }

    // NOTE: clones the whole db
    fn iter_with_prefix<'a>(
        &'a self,
        col: u32,
        prefix: &'a [u8],
    ) -> Box<dyn Iterator<Item = io::Result<DBKeyValue>> + 'a> {
        self.in_memory.iter_with_prefix(col, prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::InFile;
    use kvdb_shared_tests as st;
    use std::time::SystemTime;
    use std::{fs, io};

    fn timestramp() -> u64 {
        let dur = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH);
        dur.unwrap().as_nanos() as u64
    }
    #[test]
    fn get_fails_with_non_existing_column() -> io::Result<()> {
        let db = InFile::open(format!("{:?}", timestramp()), 1)?;
        st::test_get_fails_with_non_existing_column(&db)?;
        fs::remove_dir_all(&db.path)
    }

    #[test]
    fn put_and_get() -> io::Result<()> {
        let db = InFile::open(format!("{:?}", timestramp()), 1)?;
        st::test_put_and_get(&db)?;
        fs::remove_dir_all(&db.path)
    }

    #[test]
    fn delete_and_get() -> io::Result<()> {
        let db = InFile::open(format!("{:?}", timestramp()), 1)?;
        st::test_delete_and_get(&db)?;
        fs::remove_dir_all(&db.path)
    }

    #[test]
    fn delete_prefix() -> io::Result<()> {
        let db = InFile::open(format!("{:?}", timestramp()), st::DELETE_PREFIX_NUM_COLUMNS)?;
        st::test_delete_prefix(&db)?;
        fs::remove_dir_all(&db.path)
    }

    #[test]
    fn iter() -> io::Result<()> {
        let db = InFile::open(format!("{:?}", timestramp()), 1)?;
        st::test_iter(&db)?;
        fs::remove_dir_all(&db.path)
    }

    #[test]
    fn iter_with_prefix() -> io::Result<()> {
        let db = InFile::open(format!("{:?}", timestramp()), 1)?;
        st::test_iter_with_prefix(&db)?;
        fs::remove_dir_all(&db.path)
    }

    #[test]
    fn complex() -> io::Result<()> {
        let db = InFile::open(format!("{:?}", timestramp()), 1)?;
        st::test_complex(&db)?;
        fs::remove_dir_all(&db.path)
    }
}
