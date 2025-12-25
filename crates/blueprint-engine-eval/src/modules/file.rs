use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{
    check_fs_delete, check_fs_read, check_fs_write,
    validation::{get_string_arg, require_args},
    BlueprintError, NativeFunction, Result, Value,
};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("read_file", read_file),
        NativeFunction::new("write_file", write_file),
        NativeFunction::new("append_file", append_file),
        NativeFunction::new("exists", exists),
        NativeFunction::new("is_file", is_file),
        NativeFunction::new("is_dir", is_dir),
        NativeFunction::new("glob", glob_fn),
        NativeFunction::new("mkdir", mkdir),
        NativeFunction::new("rm", rm),
        NativeFunction::new("cp", cp),
        NativeFunction::new("mv", mv),
        NativeFunction::new("readdir", readdir),
        NativeFunction::new("basename", basename),
        NativeFunction::new("dirname", dirname),
        NativeFunction::new("abspath", abspath),
    ]
}

async fn read_file(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.read_file", &args, 1)?;
    let path = get_string_arg("file.read_file", &args, 0)?;
    check_fs_read(&path).await?;

    let content = fs::read_to_string(&path)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    Ok(Value::String(Arc::new(content)))
}

async fn write_file(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.write_file", &args, 2)?;
    let path = get_string_arg("file.write_file", &args, 0)?;
    check_fs_write(&path).await?;
    let content = get_string_arg("file.write_file", &args, 1)?;

    fs::write(&path, &content)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    Ok(Value::None)
}

async fn append_file(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.append_file", &args, 2)?;
    let path = get_string_arg("file.append_file", &args, 0)?;
    check_fs_write(&path).await?;
    let content = get_string_arg("file.append_file", &args, 1)?;

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    file.write_all(content.as_bytes())
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    Ok(Value::None)
}

async fn exists(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.exists", &args, 1)?;
    let path = get_string_arg("file.exists", &args, 0)?;
    check_fs_read(&path).await?;

    let exists = fs::try_exists(&path).await.unwrap_or(false);

    Ok(Value::Bool(exists))
}

async fn is_file(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.is_file", &args, 1)?;
    let path = get_string_arg("file.is_file", &args, 0)?;
    check_fs_read(&path).await?;

    let is_file = fs::metadata(&path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false);

    Ok(Value::Bool(is_file))
}

async fn is_dir(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.is_dir", &args, 1)?;
    let path = get_string_arg("file.is_dir", &args, 0)?;
    check_fs_read(&path).await?;

    let is_dir = fs::metadata(&path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false);

    Ok(Value::Bool(is_dir))
}

async fn glob_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.glob", &args, 1)?;
    let pattern = get_string_arg("file.glob", &args, 0)?;
    check_fs_read(&pattern).await?;

    let paths: Vec<Value> = glob::glob(&pattern)
        .map_err(|e| BlueprintError::GlobError {
            message: e.to_string(),
        })?
        .filter_map(|r| r.ok())
        .map(|p| Value::String(Arc::new(p.to_string_lossy().to_string())))
        .collect();

    Ok(Value::List(Arc::new(RwLock::new(paths))))
}

async fn mkdir(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.mkdir", &args, 1)?;
    let path = get_string_arg("file.mkdir", &args, 0)?;
    check_fs_write(&path).await?;

    fs::create_dir_all(&path)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    Ok(Value::None)
}

async fn rm(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.rm", &args, 1)?;
    let path = get_string_arg("file.rm", &args, 0)?;
    check_fs_delete(&path).await?;

    let metadata = fs::metadata(&path)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    if metadata.is_dir() {
        fs::remove_dir_all(&path).await
    } else {
        fs::remove_file(&path).await
    }
    .map_err(|e| BlueprintError::IoError {
        path: path.clone(),
        message: e.to_string(),
    })?;

    Ok(Value::None)
}

async fn cp(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.cp", &args, 2)?;
    let src = get_string_arg("file.cp", &args, 0)?;
    let dst = get_string_arg("file.cp", &args, 1)?;
    check_fs_read(&src).await?;
    check_fs_write(&dst).await?;

    fs::copy(&src, &dst)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: format!("{} -> {}", src, dst),
            message: e.to_string(),
        })?;

    Ok(Value::None)
}

async fn mv(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.mv", &args, 2)?;
    let src = get_string_arg("file.mv", &args, 0)?;
    let dst = get_string_arg("file.mv", &args, 1)?;
    check_fs_read(&src).await?;
    check_fs_write(&dst).await?;
    check_fs_delete(&src).await?;

    fs::rename(&src, &dst)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: format!("{} -> {}", src, dst),
            message: e.to_string(),
        })?;

    Ok(Value::None)
}

async fn readdir(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.readdir", &args, 1)?;
    let path = get_string_arg("file.readdir", &args, 0)?;
    check_fs_read(&path).await?;

    let mut entries = fs::read_dir(&path)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    let mut names = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?
    {
        names.push(Value::String(Arc::new(
            entry.file_name().to_string_lossy().to_string(),
        )));
    }

    Ok(Value::List(Arc::new(RwLock::new(names))))
}

async fn basename(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.basename", &args, 1)?;
    let path = get_string_arg("file.basename", &args, 0)?;
    let name = std::path::Path::new(&path)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    Ok(Value::String(Arc::new(name)))
}

async fn dirname(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.dirname", &args, 1)?;
    let path = get_string_arg("file.dirname", &args, 0)?;
    let dir = std::path::Path::new(&path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    Ok(Value::String(Arc::new(dir)))
}

async fn abspath(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("file.abspath", &args, 1)?;
    let path = get_string_arg("file.abspath", &args, 0)?;
    let abs = std::fs::canonicalize(&path)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or(path);

    Ok(Value::String(Arc::new(abs)))
}
