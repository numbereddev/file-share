use std::{
    env,
    num::NonZeroU32,
    time::{SystemTime, SystemTimeError, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha1::Sha1;
use uuid::Uuid;

pub(crate) struct Turn {
    urls: Vec<String>,
    secret: String,
    ttl_seconds: u32,
}

#[derive(Serialize)]
pub(crate) struct IceServer<'a> {
    urls: &'a [String],
    username: String,
    credential: String,
}

impl Turn {
    pub(crate) fn from_env() -> Result<Option<Self>, String> {
        fn optional(key: &str) -> Result<Option<String>, String> {
            match env::var(key) {
                Ok(value) => Ok((!value.is_empty()).then_some(value)),
                Err(env::VarError::NotPresent) => Ok(None),
                Err(err) => Err(format!("{key}: {err}")),
            }
        }

        let (urls, secret) = match (optional("TURN_URLS")?, optional("TURN_SECRET")?) {
            (None, None) => return Ok(None),
            (Some(urls), Some(secret)) => (urls, secret),
            _ => return Err("TURN_URLS and a nonempty TURN_SECRET must be set together".into()),
        };
        if urls.len() > 4096 {
            return Err("TURN_URLS must be at most 4096 bytes".into());
        }
        let urls = urls
            .split(',')
            .map(str::trim)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if urls.iter().any(|url| {
            !matches!(url.split_once(':'), Some(("turn" | "turns", address)) if !address.is_empty())
                || url.chars().any(|c| c.is_whitespace() || c.is_control())
        }) {
            return Err("TURN_URLS must be a comma-separated list of turn: or turns: URLs".into());
        }
        let ttl_seconds = match optional("TURN_TTL_SECONDS")? {
            Some(value) => value
                .parse::<NonZeroU32>()
                .map_err(|_| "TURN_TTL_SECONDS must be a positive integer (up to 4294967295)")?
                .get(),
            None => 86_400,
        };
        Ok(Some(Self {
            urls,
            secret,
            ttl_seconds,
        }))
    }

    pub(crate) fn ice_server(&self, peer: Uuid) -> Result<IceServer<'_>, SystemTimeError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        Ok(self.ice_server_at(peer, now))
    }

    fn ice_server_at(&self, peer: Uuid, now: u64) -> IceServer<'_> {
        let username = format!("{}:{peer}", now + u64::from(self.ttl_seconds));
        let mut mac = Hmac::<Sha1>::new_from_slice(self.secret.as_bytes())
            .expect("HMAC accepts any key length");
        mac.update(username.as_bytes());
        let credential = STANDARD.encode(mac.finalize().into_bytes());
        IceServer {
            urls: &self.urls,
            username,
            credential,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issues_coturn_rest_credentials() {
        let turn = Turn {
            urls: vec![
                "turn:relay.example:3478".into(),
                "turns:relay.example:5349".into(),
            ],
            secret: "shared secret".into(),
            ttl_seconds: 60,
        };
        let peer = Uuid::nil();
        let server = turn.ice_server_at(peer, 1_700_000_000);
        assert_eq!(server.urls, turn.urls);
        assert_eq!(
            server.username,
            "1700000060:00000000-0000-0000-0000-000000000000"
        );
        // Independent Base64(HMAC-SHA1) test vector for the username above.
        assert_eq!(server.credential, "mO+r3DW9H+FAO2Xzj5g913Gz7hs=");
    }
}
