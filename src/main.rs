use anyhow::{Context, Result};
use glob::glob;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::fs::*;
use std::io::prelude::*;
use std::io::BufWriter;
use std::path::*;
use tera::Tera;

use tracing::{debug, error, info, span, trace, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// context
macro_rules! ctx {
	() => { || format!("{}:{}", file!(), line!()) };
	($msg:literal $(,)?) => { || format!("{}:{} {}", file!(), line!(), $msg) };
	($fmt:expr, $($arg:tt)*) => { || format!("{}:{} {}", file!(), line!(), format!($fmt, $($arg)*)) };
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct DBParams {
    username: Option<String>,
    host: String,
    database: String,
}

fn ret_true() -> bool {
    true
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Page {
    filename: String,
    template: String,
    #[serde(default)]
    direct_vars: Vec<String>,
    #[serde(default)]
    vars: HashMap<String, String>,
    filename_params: Option<String>,

    #[serde(default = "ret_true")]
    enabled: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Config {
    params: DBParams,
    #[serde(default)]
    pages: Vec<Page>,

    #[serde(default)]
    extra_templates: Vec<String>,

    #[serde(default)]
    sql_files: Vec<String>,


    #[serde(default)]
    every_page_vars: HashMap<String, String>,
}

fn path_push(p: &Path, pp: impl AsRef<Path>) -> PathBuf {
    let mut p = p.to_path_buf();
    p.push(pp);
    p
}

#[tracing::instrument]
fn empty_dir(p: impl AsRef<Path> + Debug) -> Result<()> {
    let p: &Path = p.as_ref();

    if p.exists() {
        // empty the dir
        for entry in fs::read_dir(&p)? {
            let entry = entry?.path();
            if entry.is_dir() {
                fs::remove_dir_all(&entry).with_context(ctx!("Can't clean old output dir"))?;
                trace!(?entry, "Removing dir");
            } else {
                fs::remove_file(&entry).with_context(ctx!("Can't clean old output dir"))?;
                trace!(?entry, "Removing file");
            }
        }
        info!("Removed all output");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("start");

    let input_dir = PathBuf::from(
        std::env::args()
            .nth(1)
            .with_context(ctx!("arg 1 for source dir missing"))?,
    );
    info!(?input_dir);
    let output_directory = PathBuf::from(
        std::env::args()
            .nth(2)
            .with_context(ctx!("arg 2 for output dir missing"))?,
    );
    info!(?output_directory);

    let file = File::open(path_push(&input_dir, "config.yaml")).with_context(ctx!(
        "Trying to open {}/config.yaml",
        input_dir.to_str().unwrap()
    ))?;
    let config: Config =
        serde_yaml::from_reader(file).with_context(ctx!("Can't parse yaml from config.yaml"))?;
    info!("loaded config");

    empty_dir(&output_directory)?;

    let static_dir = path_push(&input_dir, "static");
    if static_dir.exists() {
        let mut co = fs_extra::dir::CopyOptions::new();
        co.content_only = true;
        let span = span!(Level::INFO, "Copying static dir");
        let _guard = span.enter();
        fs_extra::dir::copy(static_dir, &output_directory, &co)?;
    }

    std::fs::create_dir_all(&output_directory).with_context(ctx!("Can't create new output dir"))?;

    let mut conn_config = tokio_postgres::Config::new();
    conn_config.dbname(&config.params.database);
    conn_config.host(&config.params.host);
    conn_config.application_name("sqltatic");
    conn_config.user(
        config
            .params
            .username
            .map_or_else(|| username::get_user_name(), |s| Ok(s))?
            .as_str(),
    );

    let (mut client, connection) = conn_config.connect(tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    debug!("connected to postgresql");

    // run extra SQLs
    if !config.sql_files.is_empty() {
        let transaction = client.transaction().await?;
        let span = span!(Level::INFO, "Custom SQL");
        let _guard_sql = span.enter();
        debug!(
            "There are {} extra SQL files to run",
            config.sql_files.len()
        );
        for sql_glob in config.sql_files {
            debug!(?sql_glob, "Starting looking at glob");
            for entry in glob(
                path_push(&input_dir, sql_glob)
                    .to_str()
                    .with_context(ctx!("Converting path to str"))?,
            )? {
                let entry = entry?;
                debug!(?entry, "Reading file");
                let sql = std::fs::read_to_string(&entry)
                    .with_context(ctx!("Reading file {}", entry.to_str().unwrap()))?;
                debug!(?entry, "Running sql file");
                debug!(?sql);
                transaction
                    .batch_execute(&sql)
                    .await
                    .with_context(ctx!("Executing SQL from {}", entry.to_str().unwrap()))?;
                debug!(?entry, "finished");
            }
        }
        debug!("Committing any changes");
        transaction.commit().await?;
        drop(_guard_sql);
    }

    let mut tera = Tera::default();

    let span = span!(Level::INFO, "Parsing templates");
    let _guard = span.enter();
    tera.add_template_files(
        config
            .pages
            .iter()
            .filter_map(|pge| {
                if pge.enabled {
                    Some(&pge.template)
                } else {
                    None
                }
            })
            .chain(config.extra_templates.iter())
            .map(|filename| {
                let mut p = input_dir.clone();
                p.push("templates");
                p.push(filename);
                debug!(?filename);
                (p, Some(filename))
            })
            .collect::<Vec<_>>(),
    )
    .with_context(ctx!("trying to load all templates"))?;
    debug!("loaded all templates");
    drop(_guard);

    let mut global_base_tera_context = tera::Context::new();
    for (varname, sql) in config.every_page_vars.iter() {
        let res = client.query(sql.as_str(), &[]).await?;
        let params = pg_results_to_vec_of_values(&res)?;
        global_base_tera_context.insert(varname, &params);
    }


    if config.pages.is_empty() {
        warn!("You have not specified any pages. Nothing to do here");
    }
    let span = span!(Level::INFO, "Pages");
    let _g = span.enter();
    for page in config.pages.iter().filter(|p| p.enabled) {
        let span = span!(Level::INFO, "pages: entry", ?page.filename);
        let _g = span.enter();
        debug!("page started");

        let mut path = output_directory.clone();

        // is this a multiple?
        if let Some(filename_params) = &page.filename_params {
            debug!(?filename_params, "MultiPage");
            trace!(sql=?filename_params, "SQL query");
            let res = client
                .query(filename_params.as_str(), &[])
                .await
                .with_context(ctx!("doing SQL"))?;
            let page_filename_params = pg_results_to_vec_of_values(&res)?;
            debug!(?page_filename_params);

            for page_filename_param_value in page_filename_params {
                let span = span!(Level::INFO, "Multiple page", ?page_filename_param_value);
                let _g = span.enter();
                info!(?page_filename_param_value, "Starting multipage");

                let page_filename_param_context =
                    tera::Context::from_serialize(page_filename_param_value)?;
                let filename = tera.render_str(&page.filename, &page_filename_param_context)?;
                let path = path_push(&output_directory, &filename);
                std::fs::create_dir_all(&path.parent().unwrap()).with_context(ctx!(
                    "Creating path for output file: {:?}",
                    path.parent().unwrap()
                ))?;

                let mut page_context = global_base_tera_context.clone();
                for (varname, sql_tmpl) in page.vars.iter() {
                    trace!(var=?varname, "Page variable");
                    let sql = tera.render_str(sql_tmpl, &page_filename_param_context)?;

                    trace!(sql=?sql, "SQL query");
                    let res = client.query(sql.as_str(), &[]).await?;

                    let params = pg_results_to_vec_of_values(&res)?;
                    page_context.insert(varname, &params);
                }

                for sql_tmpl in page.direct_vars.iter() {
                    let sql = tera.render_str(sql_tmpl, &page_filename_param_context)?;
                    trace!(sql=?sql, "SQL query");
                    let res = client.query(sql.as_str(), &[]).await?;
                    let params = pg_results_to_vec_of_values(&res)?;
                    for (name, value) in params[0].as_object().unwrap() {
                        page_context.insert(name, value);
                    }
                }

                debug!(?page.template, ?filename, "Rendering template");
                let final_html = tera.render(&page.template, &page_context)?;

                let mut output_file = BufWriter::new(
                    File::create(&path).with_context(ctx!("creating output file {:?}", path))?,
                );
                info!(?path, "Writing file");
                write!(&mut output_file, "{}", final_html)
                    .with_context(ctx!("writing the contents to {:?}", path))?;
            }
        } else {
            debug!("Single page");

            path.push(&page.filename);
            let mut page_context = global_base_tera_context.clone();

            for (varname, sql) in &page.vars {
                let span = span!(Level::INFO, "Variable", ?varname);
                let _g = span.enter();

                debug!(?sql, "Starting SQL query");
                let res = client.query(sql.as_str(), &[]).await?;
                debug!(num_rows = res.len(), "sql query finished");

                let params = pg_results_to_vec_of_values(&res)?;
                debug!("converted from pg type to json");
                page_context.insert(varname, &params);
            }

            debug!(?page.template, ?path, "Rendering template");
            let final_html = tera.render(&page.template, &page_context)?;
            debug!("rendered HTML");

            let mut output_file = BufWriter::new(
                File::create(&path).with_context(ctx!("creating output file {:?}", path))?,
            );
            info!(?path, "Writing file");
            write!(&mut output_file, "{}", final_html)
                .with_context(ctx!("writing the contents to {:?}", path))?;
        }

        //future::ready(Ok(())
    }

    info!("Finished");
    Ok(())
}

fn pg_results_to_vec_of_values(
    query_results: &[tokio_postgres::Row],
) -> Result<Vec<serde_json::Value>> {
    query_results
        .iter()
        .map(|row| {
            let mut hm = serde_json::map::Map::with_capacity(row.columns().len());
            for c in row.columns().iter() {
                hm.insert(c.name().to_string(), pg_type_to_json_value(row, c.name())?);
            }
            Ok(serde_json::Value::Object(hm))
        })
        .collect::<Result<Vec<_>>>()
}

fn pg_type_to_json_value(row: &tokio_postgres::Row, i: &str) -> Result<serde_json::Value> {
    if let Ok(val) = row.try_get::<_, bool>(&i) {
        Ok(serde_json::Value::Bool(val))
    } else if let Ok(val) = row.try_get::<_, &str>(&i) {
        Ok(serde_json::Value::String(val.to_string()))
    } else if let Ok(val) = row.try_get::<_, String>(&i) {
        Ok(serde_json::Value::String(val))
    } else if let Ok(val) = row.try_get::<_, i8>(&i) {
        Ok(serde_json::Value::Number(val.into()))
    } else if let Ok(val) = row.try_get::<_, i16>(&i) {
        Ok(serde_json::Value::Number(val.into()))
    } else if let Ok(val) = row.try_get::<_, i32>(&i) {
        Ok(serde_json::Value::Number(val.into()))
    } else if let Ok(val) = row.try_get::<_, i64>(&i) {
        Ok(serde_json::Value::Number(val.into()))
    } else if let Ok(val) = row.try_get::<_, f64>(&i) {
        Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(val).context("Couldn't convert to Number")?,
        ))
    } else if let Ok(val) = row.try_get::<_, f32>(&i) {
        Ok(serde_json::Value::Number(
            serde_json::Number::from_f64(val.into()).context("Couldn't convert to Number")?,
        ))
    } else if let Ok(val) = row.try_get::<_, HashMap<String, Option<String>>>(&i) {
        let val = val
            .into_iter()
            .map(|(k, ov)| {
                ov.map(|v| (k, serde_json::Value::String(v)))
                    .map_or(Err(anyhow::anyhow!("hstore has null key")), |v| Ok(v))
            })
            .collect::<Result<serde_json::Map<_, _>>>()?;
        Ok(serde_json::Value::Object(val))
    } else if let Ok(None) = row.try_get::<_, Option<&str>>(&i) {
        Ok(serde_json::Value::Null)
    } else if let Ok(None) = row.try_get::<_, Option<i8>>(&i) {
        Ok(serde_json::Value::Null)
    } else if let Ok(None) = row.try_get::<_, Option<i16>>(&i) {
        Ok(serde_json::Value::Null)
    } else if let Ok(None) = row.try_get::<_, Option<i32>>(&i) {
        Ok(serde_json::Value::Null)
    } else if let Ok(None) = row.try_get::<_, Option<i64>>(&i) {
        Ok(serde_json::Value::Null)
    } else {
        error!(
            "Unsupported type idx: '{}' columns: {:?}",
            i,
            row.columns().iter().find(|c| c.name() == i)
        );
        anyhow::bail!(
            "Unsupported type idx: '{}' columns: {:?}",
            i,
            row.columns().iter().find(|c| c.name() == i)
        );
    }
}
