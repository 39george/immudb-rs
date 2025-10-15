use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;

#[derive(Clone)]
pub struct SessionInterceptor {
    _server_uuid: String,
    session_id: MetadataValue<Ascii>,
}

impl SessionInterceptor {
    pub fn new(session_id: String, server_uuid: String) -> Self {
        let session_id_value = MetadataValue::try_from(session_id)
            .expect("Session ID must be valid ASCII");
        Self {
            session_id: session_id_value,
            _server_uuid: server_uuid,
        }
    }
}

impl Interceptor for SessionInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> tonic::Result<tonic::Request<()>> {
        req.metadata_mut()
            .insert("sessionid", self.session_id.clone());
        Ok(req)
    }
}
