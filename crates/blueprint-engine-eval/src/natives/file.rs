use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{
    BlueprintError, NativeFunction, Result, Value,
    check_fs_read, check_fs_write, check_fs_delete,
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
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("read_file() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
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
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("write_file() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    check_fs_write(&path).await?;

    let content = args[1].as_string()?;

    fs::write(&path, &content)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    Ok(Value::None)
}

async fn append_file(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("append_file() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    check_fs_write(&path).await?;

    let content = args[1].as_string()?;

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
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("exists() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    check_fs_read(&path).await?;

    let exists = fs::try_exists(&path).await.unwrap_or(false);

    Ok(Value::Bool(exists))
}

async fn is_file(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("is_file() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    check_fs_read(&path).await?;

    let is_file = fs::metadata(&path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false);

    Ok(Value::Bool(is_file))
}

async fn is_dir(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("is_dir() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    check_fs_read(&path).await?;

    let is_dir = fs::metadata(&path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false);

    Ok(Value::Bool(is_dir))
}

async fn glob_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("glob() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let pattern = args[0].as_string()?;
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
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("mkdir() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
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
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("rm() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
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
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("cp() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let src = args[0].as_string()?;
    let dst = args[1].as_string()?;
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
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("mv() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let src = args[0].as_string()?;
    let dst = args[1].as_string()?;
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
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("readdir() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    check_fs_read(&path).await?;

    let mut entries = fs::read_dir(&path)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await.map_err(|e| BlueprintError::IoError {
        path: path.clone(),
        message: e.to_string(),
    })? {
        names.push(Value::String(Arc::new(
            entry.file_name().to_string_lossy().to_string(),
        )));
    }

    Ok(Value::List(Arc::new(RwLock::new(names))))
}

async fn basename(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("basename() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    let name = std::path::Path::new(&path)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    Ok(Value::String(Arc::new(name)))
}

async fn dirname(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("dirname() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    let dir = std::path::Path::new(&path)
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    Ok(Value::String(Arc::new(dir)))
}

async fn abspath(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("abspath() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let path = args[0].as_string()?;
    let abs = std::fs::canonicalize(&path)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or(path);

    Ok(Value::String(Arc::new(abs)))
}
