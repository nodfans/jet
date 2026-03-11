use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use jet_proto::proto::repository_service_server::RepositoryServiceServer;
use jet_server::{AuthConfig, JetServer, RepoPermissions};
use serde::Deserialize;

const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "jet-server", version, about = "Jet gRPC repository server")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:4220")]
    listen: SocketAddr,
    #[arg(long, default_value = ".")]
    repos_root: PathBuf,
    #[arg(long)]
    auth_config: Option<PathBuf>,
    #[arg(long, value_delimiter = ',', value_name = "IDENTITY:TOKEN")]
    auth_token: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AuthConfigFile {
    #[serde(default)]
    users: Vec<AuthUserConfig>,
    #[serde(default, rename = "repos")]
    repo_permissions: Vec<AuthRepoConfig>,
}

#[derive(Debug, Deserialize)]
struct AuthUserConfig {
    name: String,
    #[serde(default)]
    tokens: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AuthRepoConfig {
    name: String,
    #[serde(default)]
    read: Vec<String>,
    #[serde(default)]
    write: Vec<String>,
    #[serde(default)]
    admin: Vec<String>,
}

#[derive(Default)]
struct AuthValidationState {
    users: BTreeSet<String>,
    tokens: BTreeMap<String, String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut auth_entries = load_auth_entries(cli.auth_config.as_deref())?;
    let auth_values = if cli.auth_token.is_empty() {
        std::env::var("JET_AUTH_TOKENS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    } else {
        cli.auth_token.clone()
    };
    auth_entries
        .identities
        .extend(parse_auth_token_entries(&auth_values)?);
    let auth = AuthConfig::with_repo_permissions(auth_entries.identities, auth_entries.repos);
    let service = JetServer::with_auth(&cli.repos_root, auth)?;

    println!(
        "Jet server listening on {} serving repos from {}",
        cli.listen,
        cli.repos_root.display()
    );

    tonic::transport::Server::builder()
        .add_service(
            RepositoryServiceServer::new(service)
                .max_decoding_message_size(MAX_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_MESSAGE_BYTES),
        )
        .serve(cli.listen)
        .await?;

    Ok(())
}

#[derive(Default)]
struct ParsedAuthEntries {
    identities: Vec<(String, String)>,
    repos: Vec<(String, RepoPermissions)>,
}

fn load_auth_entries(path: Option<&std::path::Path>) -> anyhow::Result<ParsedAuthEntries> {
    let Some(path) = path else {
        return Ok(ParsedAuthEntries::default());
    };
    let data = std::fs::read_to_string(path)?;
    let parsed: AuthConfigFile = toml::from_str(&data)?;
    validate_auth_config_file(&parsed)?;
    let identities = parsed
        .users
        .into_iter()
        .flat_map(|user| {
            user.tokens
                .into_iter()
                .map(move |token| (user.name.clone(), token))
        })
        .collect::<Vec<_>>();
    let repos = parsed
        .repo_permissions
        .into_iter()
        .map(|repo| {
            (
                repo.name,
                RepoPermissions {
                    read: repo.read.into_iter().collect(),
                    write: repo.write.into_iter().collect(),
                    admin: repo.admin.into_iter().collect(),
                },
            )
        })
        .collect::<Vec<_>>();
    Ok(ParsedAuthEntries { identities, repos })
}

fn parse_auth_token_entries(values: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    let mut entries = Vec::new();
    for value in values {
        let (identity, token) = value
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid auth token mapping `{value}`; expected identity:token"))?;
        let identity = identity.trim();
        let token = token.trim();
        if identity.is_empty() || token.is_empty() {
            return Err(anyhow::anyhow!(
                "invalid auth token mapping `{value}`; expected identity:token"
            ));
        }
        entries.push((identity.to_string(), token.to_string()));
    }
    Ok(entries)
}

fn validate_auth_config_file(config: &AuthConfigFile) -> anyhow::Result<()> {
    let mut state = AuthValidationState::default();

    for user in &config.users {
        let name = user.name.trim();
        if name.is_empty() {
            return Err(anyhow::anyhow!("auth config contains a user with an empty name"));
        }
        if !state.users.insert(name.to_string()) {
            return Err(anyhow::anyhow!("auth config contains duplicate user `{name}`"));
        }
        if user.tokens.is_empty() {
            return Err(anyhow::anyhow!("auth config user `{name}` has no tokens"));
        }
        for token in &user.tokens {
            let token = token.trim();
            if token.is_empty() {
                return Err(anyhow::anyhow!("auth config user `{name}` has an empty token"));
            }
            if let Some(existing) = state.tokens.insert(token.to_string(), name.to_string()) {
                return Err(anyhow::anyhow!(
                    "auth config token is reused by multiple users: `{existing}` and `{name}`"
                ));
            }
        }
    }

    for repo in &config.repo_permissions {
        let repo_name = repo.name.trim();
        if repo_name.is_empty() {
            return Err(anyhow::anyhow!("auth config contains a repo with an empty name"));
        }
        for identity in repo
            .read
            .iter()
            .chain(repo.write.iter())
            .chain(repo.admin.iter())
        {
            let identity = identity.trim();
            if identity.is_empty() {
                return Err(anyhow::anyhow!(
                    "auth config repo `{repo_name}` contains an empty identity"
                ));
            }
            if !state.users.contains(identity) {
                return Err(anyhow::anyhow!(
                    "auth config repo `{repo_name}` references unknown user `{identity}`"
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_tokens() {
        let config = AuthConfigFile {
            users: vec![
                AuthUserConfig {
                    name: "alice".to_string(),
                    tokens: vec!["shared".to_string()],
                },
                AuthUserConfig {
                    name: "bob".to_string(),
                    tokens: vec!["shared".to_string()],
                },
            ],
            repo_permissions: vec![],
        };

        let err = validate_auth_config_file(&config).expect_err("duplicate token should fail");
        assert!(err.to_string().contains("token is reused"));
    }

    #[test]
    fn rejects_unknown_acl_user() {
        let config = AuthConfigFile {
            users: vec![AuthUserConfig {
                name: "alice".to_string(),
                tokens: vec!["token".to_string()],
            }],
            repo_permissions: vec![AuthRepoConfig {
                name: "game".to_string(),
                read: vec!["bob".to_string()],
                write: vec![],
                admin: vec![],
            }],
        };

        let err = validate_auth_config_file(&config).expect_err("unknown acl user should fail");
        assert!(err.to_string().contains("unknown user `bob`"));
    }
}
