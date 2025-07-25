use std::{io::IsTerminal, path::PathBuf};

use anyhow::{Error, Result};
use clap::{arg, Parser, Subcommand};
use sha2::{Digest, Sha256};

use crate::share;

#[derive(Parser, Debug)]
#[command(name = "stigmerge")]
#[command(bin_name = "stigmerge")]
pub struct Cli {
    #[arg(long, env)]
    pub no_ui: bool,

    #[arg(long, env)]
    pub state_dir: Option<String>,

    #[command(subcommand)]
    pub commands: Commands,
}

impl Cli {
    pub fn no_ui(&self) -> bool {
        return self.no_ui || !std::io::stdout().is_terminal();
    }

    pub fn state_dir(&self) -> Result<String> {
        if let Some(s) = &self.state_dir {
            return Ok(s.to_owned());
        }
        match self.commands {
            Commands::Fetch {
                ref share_keys,
                ref index_digest,
                ref output_path,
                ..
            } => {
                if share_keys.len() == 0 {
                    return Err(Error::msg("at least one share key must be provided"));
                }
                let key_match = match index_digest {
                    Some(digest) => digest,
                    None => &share_keys[0],
                };
                self.state_dir_for(format!(
                    "get:{}:{}",
                    key_match,
                    output_path.to_string_lossy()
                ))
            }
            Commands::Seed { ref path } => {
                self.state_dir_for(format!("seed:{}", path.to_string_lossy()))
            }
            _ => Err(Error::msg("invalid command")),
        }
    }

    pub fn state_dir_for(&self, key: String) -> Result<String> {
        let mut key_digest = Sha256::new();
        key_digest.update(&key.as_bytes());
        let key_digest_bytes: [u8; 32] = key_digest.finalize().into();
        let dir_name = hex::encode(key_digest_bytes);
        let data_dir = dirs::state_dir()
            .or(dirs::data_local_dir())
            .ok_or(Error::msg("cannot resolve state dir"))?;
        let state_dir = data_dir
            .join("stigmerge")
            .join(dir_name)
            .into_os_string()
            .into_string()
            .map_err(|os| Error::msg(format!("{:?}", os)))?;
        Ok(state_dir)
    }

    pub fn version(&self) -> bool {
        if let Commands::Version = self.commands {
            return true;
        }
        return false;
    }
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Fetch {
        share_keys: Vec<String>,

        #[arg(long = "index-digest", short = 'i')]
        index_digest: Option<String>,

        #[arg(long = "output-path", short = 'o', default_value = ".")]
        output_path: PathBuf,

        #[arg(long = "fetchers", short = 'n', default_value = "50")]
        fetchers: usize,
    },
    Seed {
        path: PathBuf,
    },
    Version,
}

impl Commands {
    pub fn share_args(&self) -> Result<(share::Mode, share::Config)> {
        Ok(match self {
            Commands::Fetch {
                share_keys: share_key_strings,
                index_digest,
                output_path,
                fetchers,
            } => {
                let want_index_digest = match index_digest {
                    Some(digest_string) => {
                        let digest = hex::decode(digest_string)?;
                        Some(
                            digest
                                .try_into()
                                .map_err(|_| Error::msg("invalid digest length"))?,
                        )
                    }
                    None => None,
                };
                let mut share_keys = vec![];
                for share_key_string in share_key_strings {
                    share_keys.push(share_key_string.parse()?);
                }
                (
                    share::Mode::Fetch {
                        root: output_path.into(),
                        want_index_digest,
                        share_keys,
                    },
                    share::Config {
                        n_fetchers: TryInto::<u8>::try_into(*fetchers)?,
                    },
                )
            }
            Commands::Seed { path } => (
                share::Mode::Seed {
                    path: path.to_owned(),
                },
                share::Config::default(),
            ),
            Commands::Version => return Err(Error::msg("invalid command")),
        })
    }
}
