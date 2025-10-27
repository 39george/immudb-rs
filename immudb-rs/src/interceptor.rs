use std::sync::{Arc, RwLock};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;

use crate::error::Error;

struct SessionState {
    server_uuid: MetadataValue<Ascii>,
    session_id: MetadataValue<Ascii>,
    db_token: RwLock<Option<MetadataValue<Ascii>>>,
}

#[derive(Clone)]
pub struct SessionInterceptor {
    state: Arc<SessionState>,
}

impl SessionInterceptor {
    pub fn new(session_id: &str, server_uuid: &str) -> Self {
        let sid =
            MetadataValue::try_from(session_id).expect("ascii session id");
        let su =
            MetadataValue::try_from(server_uuid).expect("ascii server uuid");
        Self {
            state: Arc::new(SessionState {
                server_uuid: su,
                session_id: sid,
                db_token: RwLock::new(None),
            }),
        }
    }

    pub fn set_token(&self, token: String) -> crate::Result<()> {
        let mv = MetadataValue::try_from(token)
            .map_err(|e| Error::InvalidInput(format!("ascii token: {e:?}")))?;
        *self.state.db_token.write().unwrap() = Some(mv);
        Ok(())
    }
}

impl Interceptor for SessionInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> tonic::Result<tonic::Request<()>> {
        let md = req.metadata_mut();
        md.insert("sessionid", self.state.session_id.clone());
        md.insert("immudb-uuid", self.state.server_uuid.clone());
        if let Some(tok) = self.state.db_token.read().unwrap().as_ref() {
            md.insert("authorization", tok.clone()); // <— это важно
        }
        Ok(req)
    }
}
